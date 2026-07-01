import requests
import logging
import threading
from typing import Any
from collections import UserDict

import xmltodict
from sqlalchemy import (String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime,
                        JSON, Engine, func, insert, select, update)
from sqlalchemy.dialects.postgresql import JSONB
from dbmerge import dbmerge

from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.common_functions import parse_object_full_name

logger = logging.getLogger(__name__)

# Таймауты HTTP-запросов к 1С по умолчанию: (connect, read) в секундах. requests с timeout=None
# висит бесконечно при недоступном сервере — именно поэтому процесс зависал на запросе метаданных.
# connect ограничивает ожидание установки соединения, read — ожидание ответа.
# Метаданные ($metadata) — небольшой и быстрый запрос: ждать долго смысла нет, лучше быстро упасть
# на недоступной/зависшей 1С. Чтение данных/страниц выгрузки бывает объёмным — там read больше.
# Применяется, когда request_timeout не задан явно (None).
DEFAULT_METADATA_TIMEOUT: tuple[float, float] = (60, 120)


def resolve_timeout(request_timeout: float | tuple[float, float] | None):
    """request_timeout как есть, либо default, если он не задан (None).
    Гарантирует, что ни один HTTP-запрос не уходит в requests с timeout=None (вечное ожидание)."""
    return DEFAULT_METADATA_TIMEOUT if request_timeout is None else request_timeout


# Таблица-реестр объектов 1С и состояния их полной выгрузки (см. MetadataReader1C).
METADATA_OBJECTS_TABLE = 'metadata_objects_1c'

type_mapping = {'Guid':Uuid(),
                'Int64':BigInteger(),
                'Int16':SmallInteger(),
                'String':String(),
                'Double':Numeric(),
                'Boolean':Boolean(), 
                'DateTime':DateTime()}

GUESS_UUID_TYPES = True
# Проблема в том, что 1С часть GUID полей присылает как строки в описании метаданных.
# В этом модуле есть логика, которая определяет тип UUID поля, по имени поля "Recorder" 
# или по наличию другого поля с постфиксом "_Type" для составных типов данных.
# На всякий случай сделан этот флаг, чтобы можно было эту логику отключить.
# Конечно, если отключить флаг, то это часть полей будут UUID, а часть VARCHAR.
# В этом случает VARCHAR поля лучше руками в базе поменять на UUID, 
# т.к. иначе будут медленно работать JOIN

REGISTER_TYPES = ('InformationRegister','AccumulationRegister')
ENTITY_TYPES = ('Catalog','Document')
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'
TYPE_PREFIX = 'Edm.'

# Системные поля движений регистра (не измерения/ресурсы/реквизиты).
SYSTEM_REGISTER_FIELDS = frozenset(
    ('Recorder', 'Period', 'LineNumber', 'Active', 'RecordType', 'Recorder_Type'))
# Поля period-спайна в виртуальной таблице _Turnover (агрегаты по периодам, не измерения).
TURNOVER_PERIOD_FIELDS = frozenset(
    ('Period', 'SecondPeriod', 'MinutePeriod', 'HourPeriod', 'DayPeriod', 'WeekPeriod',
     'TenDaysPeriod', 'MonthPeriod', 'QuarterPeriod', 'HalfYearPeriod', 'YearPeriod'))
TURNOVER_RESOURCE_SUFFIXES = ('Turnover', 'Receipt', 'Expense')

def _check_object_is_table_part(base_name:str, complextypes: dict[str, list[str]]):
    """
    Если найден блок метаданных с таким же именем и с постфиксом _RowType, значит это табличная часть
    """
    row_type = complextypes.get(base_name + '_RowType')
    return row_type is not None

def _classify_register_fields(base_name: str, properties: dict, complextypes: dict[str, list[str]]):
    """
    Делит поля движений регистра на измерения / ресурсы / реквизиты, сравнивая с виртуальными
    таблицами _Balance / _Turnover (ComplexType из $metadata). Виртуальные таблицы 1С считает
    функциями на лету — здесь они нужны ТОЛЬКО как описание типов для классификации.

    Возвращает (dimensions, resources, attributes). Если функции для регистра не опубликованы
    (нет _Balance/_Turnover) — ([], [], [], None).
    """
    prop_names = set(properties)
    balance = complextypes.get(base_name + '_Balance')
    turnover = complextypes.get(base_name + '_Turnover')

    dimensions: list[str] = []
    resources: list[str] = []

    if balance is not None:
        for f in balance:
            if f.endswith('Balance'):
                resources.append(f[:-len('Balance')])
            elif not f.endswith('_Type'):
                dimensions.append(f)
    if turnover is not None:
        for f in turnover:
            suffix = next((s for s in TURNOVER_RESOURCE_SUFFIXES if f.endswith(s)), None)
            if suffix is not None:
                resources.append(f[:-len(suffix)])
            elif f in TURNOVER_PERIOD_FIELDS or f in SYSTEM_REGISTER_FIELDS or f.endswith('_Type'):
                continue
            else:
                dimensions.append(f)
    else:
        return [], [], []

    # Оставляем только реально присутствующие в движениях поля, без дублей (порядок сохраняем).
    dimensions = [d for d in dict.fromkeys(dimensions) if d in prop_names]
    resources = [r for r in dict.fromkeys(resources) if r in prop_names]
    used = set(dimensions) | set(resources) | SYSTEM_REGISTER_FIELDS
    attributes = [f for f in properties if f not in used and not f.endswith('_Type')]
    return dimensions, resources, attributes




class MetadataObject1C(UserDict):
    def __init__(self, name, properties, primary_key, object_key=None,
                 dimensions=None, resources=None, attributes=None, is_table_part = False):
        super().__init__(properties)
        self.name = name
        self.primary_key = primary_key
        # Ключ для scoped-удаления при merge (см. _get_object_key):
        # регистр -> Recorder(+Recorder_Type), табличная часть -> Ref_Key,
        # документ/справочник -> None (одна запись, delete не нужен).
        self.object_key = object_key
        # Классификация полей регистра (см. _classify_register_fields); для не-регистров пусто.
        # Имена — оригинальные (1С), маппятся NameMapper-ом при использовании, как object_key.
        self.dimensions = dimensions or []   # измерения
        self.resources = resources or []     # ресурсы
        self.attributes = attributes or []   # реквизиты

        self.is_table_part = is_table_part


    def get_column_types(self) -> dict[str, Any]:
        return {col: type_mapping[typ] for col, typ in self.data.items()}



class MetadataReader1C(UserDict):
    def __init__(self, odata_url:str, odata_auth: tuple[str, str] | None = None,
                 request_timeout: float | None = None,
                 engine: Engine | None = None, schema: str | None = None):
        super().__init__()
        self.odata_url=odata_url
        self.odata_auth=odata_auth
        self.request_timeout=request_timeout
        # В конструкторе метаданные НЕ загружаются (без сетевого запроса), чтобы недоступность 1С
        # на старте не роняла процесс. Загрузка — get_metadata(), которая выставляет is_loaded=True.
        self.is_loaded = False
        # get_metadata может вызываться лениво из фоновых потоков full_load (новый объект/поле)
        # параллельно с основным циклом — сериализуем перестроение словаря.
        self._lock = threading.Lock()

        # Реестр объектов и состояния полной выгрузки (metadata_objects_1c). Ведётся, только если
        # передан engine (в библиотечном/тестовом сценарии без БД метаданные читаются как раньше).
        # Членство в плане обмена определяется эмпирически — по приходу объекта в пакете SelectChanges
        # (require_full_load_if_new); is_deleted/merged_on ведёт dbmerge синхронизацией с $metadata.
        # Таблицу создаёт сам dbmerge при первой sync; objects_table — её Table-описание оттуда же.
        self.engine = engine
        self.schema = None if (engine is not None and engine.dialect.name == 'sqlite') else schema
        self.objects_table = None


    def _read_metadata_item_properties(self, item:dict):
        """
        Читаем поля объекта метаданных
        """
        item_properties = item.get('Property')

        properties = {}
        for item_property in item_properties:
            property_name = item_property['@Name']
            property_type = item_property['@Type']

            property_type = property_type.removeprefix(TYPE_PREFIX)

            if GUESS_UUID_TYPES:
                if property_name=='Recorder':
                    # Принудительно ставим Uuid для регистраторов, т.к. 1С почему-то присылает String
                    property_type='Guid'            

            if property_type in type_mapping:
                properties[property_name] = property_type
            else:
                if not property_type.startswith('Collection'):
                # если это collection, то значит это просто табличная часть, она будет отдельно
                # если нет, то показываем ошибку, т.к. это похоже на неизвестный тип
                    logger.error(f'Property {property_name} has unknown type {property_type}')
            
        if GUESS_UUID_TYPES:
            for property_name in properties.keys():
                # Если мы видим что есть поле с постфиксом Type, то значит
                # ищем такое же поле без постфикса, т.к. в этом случае это составной тип и нужно 
                # изменить поле на Uuid т.к. 1С полчему-то присылает String          
                if property_name.endswith('_Type'):
                    uuid_property_name = property_name.removesuffix('_Type')
                    if uuid_property_name in properties.keys():
                        properties[uuid_property_name] = 'Guid'


        return properties

    def _read_metadata_item_key(self, item:dict, properties: dict):
        """
        Читаем список ключевых полей объекта метаданных
        """
        key = {}
        item_key = (item.get('Key') or {}).get('PropertyRef')

        if item_key:
            key_fields = [k.get('@Name') for k in item_key if k.get('@Name') is not None]

        key = {k: properties[k] for k in key_fields if k in properties}

        return key


    def _get_object_key(self, item_name: str, properties: dict, primary_key: dict):
        """
        Ключ для scoped-удаления при merge (delete_condition в dbmerge).
        Изменения приходят группами, которые целиком заменяют существующие строки:
        - регистр: набор записей одного регистратора -> Recorder (+ Recorder_Type);
        - табличная часть (ключ Ref_Key + ещё поля) -> Ref_Key владельца;
        - документ/справочник (единственная запись по Ref_Key) -> None, удаление не нужно.
        """
        if item_name.startswith(REGISTER_TYPES):
            return [c for c in ('Recorder', 'Recorder_Type') if c in properties]
        if 'Ref_Key' in primary_key and len(primary_key) > 1:
            return ['Ref_Key']
        return None

    def get_metadata(self):
        """
        Запрашиваем метаданные всех доступных объектов из odata и (если задан engine) синхронизируем
        реестр metadata_objects_1c с актуальным составом $metadata.
        Можно вызывать повторно для обновления (при появлении нового объекта/поля — см. data_reader);
        под блокировкой, т.к. вызывается и из фоновых потоков full_load. В конце is_loaded=True.
        """
        with self._lock:
            self._fetch_and_parse_metadata()
            self.is_loaded = True
            if self.engine is not None:
                self._sync_objects(list(self.keys()))

    def _fetch_and_parse_metadata(self):
        logger.info('Requesting metadata from 1C ODATA')
        
        url = f'{self.odata_url}/$metadata'
        response = requests.get(url,auth=self.odata_auth,
                                timeout=resolve_timeout(self.request_timeout))
        response.raise_for_status()

        metadata = xmltodict.parse(response.text,force_list=('Property','PropertyRef','ComplexType'))
        metadata_schema = ((metadata.get('edmx:Edmx') or {}).get('edmx:DataServices') or {}).get('Schema') or {}
        metadata_entity_types = metadata_schema.get('EntityType') or []
        # Виртуальные таблицы регистров (_Balance/_Turnover) приходят как ComplexType — нужны для
        # Табличные части документов и справочников также приходят как ComplexType
        # классификации полей регистра на измерения/ресурсы/реквизиты (см. _classify_register_fields).
        complextypes = {ct.get('@Name'): [p.get('@Name') for p in (ct.get('Property') or [])]
                        for ct in (metadata_schema.get('ComplexType') or [])}


        for item in metadata_entity_types:

            item_name = item.get('@Name')

            if item_name.startswith(REGISTER_TYPES) and item_name.endswith("_RecordType"):
            # регистр с постфиксом RecordType содержит описание полей регистра и описание ключа
                item_name = item_name.removesuffix("_RecordType")
                properties = self._read_metadata_item_properties(item)
                primary_key = self._read_metadata_item_key(item,properties)
                object_key = self._get_object_key(item_name, properties, primary_key)
                dimensions, resources, attributes = _classify_register_fields(
                    item_name, properties, complextypes)
                self[item_name] = MetadataObject1C(item_name, properties, primary_key, object_key,
                                                   dimensions, resources, attributes)

            elif item_name.startswith(ENTITY_TYPES) and not item_name.endswith(METADATA_POSTFIXES):
            # если документ или справочник без постфикса, то
            # читаем его описание полей и ключ
            # (также может быть табличная часть документа или справочника)
                properties = self._read_metadata_item_properties(item)
                primary_key = self._read_metadata_item_key(item,properties)
                object_key = self._get_object_key(item_name, properties, primary_key)
                is_table_part = _check_object_is_table_part(item_name, complextypes)
                self[item_name] = MetadataObject1C(item_name, properties, primary_key, object_key, 
                                                   is_table_part=is_table_part)



    # --- Реестр объектов и состояния полной выгрузки (metadata_objects_1c) ---

    def _sync_objects(self, object_names: list[str]) -> None:
        """
        Синхронизирует реестр с актуальным составом $metadata через dbmerge (delete): новые объекты
        вставляются , пропавшие помечаются удаляются.
        Если объект пропал из метаданных, то он будет удален в таблице. 
        """
        if not object_names:
            return

        # object_type (префикс имени) — неключевая колонка. Колонки full_load передаём только
        # для создания таблицы/первой вставки и исключаем из UPDATE (skip_update_fields).
        # object_full_name_en — транслитерированное имя (= имя таблицы в БД); fields/fields_en — JSON-списки
        # полей объекта: оригинальные имена 1С и их транслит (= имена колонок в БД). Для удобного
        # просмотра состава объекта. Все три синхронизируются с $metadata.
        mapper = NameMapper1C()
        # На json-колонке dbmerge сравнивает значения через IS DISTINCT FROM; у Postgres-типа json
        # нет оператора равенства — берём jsonb (у sqlite generic JSON хранится текстом, сравнение ок).
        json_type = JSONB() if self.engine.dialect.name == 'postgresql' else JSON()
        data = []
        for object_full_name in object_names:
            obj = self.get(object_full_name)
            field_names = list(obj.keys()) if obj is not None else []
            object_name, object_type = parse_object_full_name(object_full_name)
            data.append({
                'object_full_name': object_full_name, 
                'object_full_name_en': mapper.map_object_name(object_full_name),
                'object_name': object_name,
                'object_type': object_type,
                'fields': field_names,
                'fields_en': [mapper.map_field_name(f) for f in field_names],
                # эти значения устранавливаются только при insert, из update они исключены
                'full_load_is_required': False, 'last_full_load_dt': None})
            
        with dbmerge(engine=self.engine, table_name=METADATA_OBJECTS_TABLE, data=data,
                     key=['object_full_name'], delete_mode='delete', 
                     merged_on_field='merged_on', schema=self.schema,
                     data_types={'object_full_name': String(),
                                 'object_name': String(),
                                 'object_type': String(),
                                 'object_full_name_en': String(), 
                                 'fields': json_type,
                                 'fields_en': json_type,
                                 'full_load_is_required': Boolean(), 
                                 'last_full_load_dt': DateTime()
                                 },
                     skip_update_fields=['full_load_is_required', 'last_full_load_dt']) as merge:
            merge.exec()
            self.objects_table = merge.table   # Table-описание созданной/существующей таблицы

    def require_full_load_if_new(self, object_full_name: str) -> None:
        """
        Помечает объект как требующий полной выгрузки, если он ещё ни разу не выгружался целиком
        (last_full_load_dt IS NULL). Вызывается на каждый объект пакета SelectChanges — это и есть
        признак членства в плане обмена. Если строки ещё нет (объект пришёл в пакете раньше, чем его
        увидела sync) — вставляем.
        """
        table = self.objects_table
        with self.engine.begin() as conn:
            row = conn.execute(select(table.c.last_full_load_dt)
                               .where(table.c.object_full_name == object_full_name)).first()
            if row is None:
                logger.info('reloading metadata')
                self.get_metadata()
            elif row.last_full_load_dt is None:
                conn.execute(update(table).where(table.c.object_full_name == object_full_name)
                             .values(full_load_is_required=True))

    def list_full_load_required(self) -> list[str]:
        """Объекты, ожидающие полной выгрузки (требуется и не удалён из метаданных)."""
        table = self.objects_table
        with self.engine.connect() as conn:
            return list(conn.execute(
                select(table.c.object_name)
                .where(table.c.full_load_is_required)).scalars())

    def mark_full_loaded(self, object_name: str) -> None:
        """Фиксирует успешную полную выгрузку: ставит last_full_load_dt=now(), снимает требование."""
        table = self.objects_table
        with self.engine.begin() as conn:
            conn.execute(update(table).where(table.c.object_name == object_name)
                         .values(last_full_load_dt=func.now(), full_load_is_required=False))