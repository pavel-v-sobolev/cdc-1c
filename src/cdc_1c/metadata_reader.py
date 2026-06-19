import requests
import logging
import threading
from typing import Any
from collections import UserDict

import xmltodict
from sqlalchemy import (String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime,
                        Engine, func, insert, select, update)
from dbmerge import dbmerge

logger = logging.getLogger(__name__)

# Таблица-реестр объектов 1С и состояния их полной выгрузки (см. MetadataReader1C).
METADATA_OBJECTS_TABLE = 'metadata_objects_1c'
MERGED_ON_FIELD = 'merged_on'
IS_DELETED_FIELD = 'is_deleted'
# Колонки состояния full_load, которыми управляем мы (а не dbmerge): передаём их в sync только для
# создания таблицы/первой вставки, но исключаем из UPDATE (skip_update_fields), чтобы синхронизация
# с $metadata не затирала флаги. last_full_load_dt — целиком NULL по началу, поэтому задаём тип явно.
FULL_LOAD_FIELDS = ('full_load_is_required', 'last_full_load_dt')


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


def _classify_register_fields(base_name: str, properties: dict, complextypes: dict[str, list[str]]):
    """
    Делит поля движений регистра на измерения / ресурсы / реквизиты, сравнивая с виртуальными
    таблицами _Balance / _Turnover (ComplexType из $metadata). Виртуальные таблицы 1С считает
    функциями на лету — здесь они нужны ТОЛЬКО как описание типов для классификации.

    Возвращает (dimensions, resources, attributes, kind). Если функции для регистра не опубликованы
    (нет _Balance/_Turnover) — ([], [], [], None).
    """
    prop_names = set(properties)
    balance = complextypes.get(base_name + '_Balance')
    turnover = complextypes.get(base_name + '_Turnover')

    dimensions: list[str] = []
    resources: list[str] = []

    if balance is not None:
        kind = 'balance'
        for f in balance:
            if f.endswith('Balance'):
                resources.append(f[:-len('Balance')])
            elif not f.endswith('_Type'):
                dimensions.append(f)
    elif turnover is not None:
        kind = 'turnover'
        for f in turnover:
            suffix = next((s for s in TURNOVER_RESOURCE_SUFFIXES if f.endswith(s)), None)
            if suffix is not None:
                resources.append(f[:-len(suffix)])
            elif f in TURNOVER_PERIOD_FIELDS or f in SYSTEM_REGISTER_FIELDS or f.endswith('_Type'):
                continue
            else:
                dimensions.append(f)
    else:
        return [], [], [], None

    # Оставляем только реально присутствующие в движениях поля, без дублей (порядок сохраняем).
    dimensions = [d for d in dict.fromkeys(dimensions) if d in prop_names]
    resources = [r for r in dict.fromkeys(resources) if r in prop_names]
    used = set(dimensions) | set(resources) | SYSTEM_REGISTER_FIELDS
    attributes = [f for f in properties if f not in used and not f.endswith('_Type')]
    return dimensions, resources, attributes, kind




class MetadataObject1C(UserDict):
    def __init__(self, properties, primary_key, object_key=None,
                 dimensions=None, resources=None, attributes=None, kind=None):
        super().__init__(properties)
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
        self.kind = kind                     # 'balance' | 'turnover' | None

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
        response = requests.get(url,auth=self.odata_auth,timeout=self.request_timeout)
        response.raise_for_status()

        metadata = xmltodict.parse(response.text,force_list=('Property','PropertyRef','ComplexType'))
        metadata_schema = ((metadata.get('edmx:Edmx') or {}).get('edmx:DataServices') or {}).get('Schema') or {}
        metadata_entity_types = metadata_schema.get('EntityType') or []
        # Виртуальные таблицы регистров (_Balance/_Turnover) приходят как ComplexType — нужны для
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
                dimensions, resources, attributes, kind = _classify_register_fields(
                    item_name, properties, complextypes)
                self[item_name] = MetadataObject1C(properties, primary_key, object_key,
                                                   dimensions, resources, attributes, kind)

            elif item_name.startswith(ENTITY_TYPES) and not item_name.endswith(METADATA_POSTFIXES):
            # если документ или справочник без постфикса, то
            # читаем его описание полей и ключ
            # (также может быть табличная часть документа или справочника)
                properties = self._read_metadata_item_properties(item)
                primary_key = self._read_metadata_item_key(item,properties)
                object_key = self._get_object_key(item_name, properties, primary_key)
                self[item_name] = MetadataObject1C(properties,primary_key,object_key)

    # --- Реестр объектов и состояния полной выгрузки (metadata_objects_1c) ---

    def _sync_objects(self, object_names: list[str]) -> None:
        """
        Синхронизирует реестр с актуальным составом $metadata через dbmerge (mark): новые объекты
        вставляются (is_deleted=False), пропавшие помечаются is_deleted=True, вернувшиеся снимают
        пометку. У вернувшихся обнуляем last_full_load_dt и взводим full_load_is_required —
        возврат объекта требует новой полной выгрузки.
        """
        if not object_names:
            return

        # До первой sync таблицы ещё нет (её создаёт dbmerge) — тогда удалённых заведомо нет.
        prev_deleted = set()
        if self.objects_table is not None:
            with self.engine.connect() as conn:
                prev_deleted = set(conn.execute(
                    select(self.objects_table.c.object_name)
                    .where(self.objects_table.c[IS_DELETED_FIELD])).scalars())

        # object_type (префикс имени) — неключевая колонка: без неё dbmerge рано выходит из
        # update-фазы и не снимает пометку у вернувшихся объектов. Колонки full_load передаём только
        # для создания таблицы/первой вставки и исключаем из UPDATE (skip_update_fields).
        data = [{'object_name': name, 'object_type': name.split('_', 1)[0],
                 'full_load_is_required': False, 'last_full_load_dt': None} for name in object_names]
        with dbmerge(engine=self.engine, table_name=METADATA_OBJECTS_TABLE, data=data,
                     key=['object_name'], delete_mode='mark', delete_mark_field=IS_DELETED_FIELD,
                     merged_on_field=MERGED_ON_FIELD, schema=self.schema,
                     data_types={'full_load_is_required': Boolean(), 'last_full_load_dt': DateTime()},
                     skip_update_fields=list(FULL_LOAD_FIELDS)) as merge:
            merge.exec()
            self.objects_table = merge.table   # Table-описание созданной/существующей таблицы

        reappeared = prev_deleted & set(object_names)
        if reappeared:
            table = self.objects_table
            with self.engine.begin() as conn:
                conn.execute(update(table).where(table.c.object_name.in_(reappeared))
                             .values(last_full_load_dt=None, full_load_is_required=True))
            logger.info("Objects returned to metadata, full_load re-required: %s", sorted(reappeared))

    def require_full_load_if_new(self, object_name: str) -> None:
        """
        Помечает объект как требующий полной выгрузки, если он ещё ни разу не выгружался целиком
        (last_full_load_dt IS NULL). Вызывается на каждый объект пакета SelectChanges — это и есть
        признак членства в плане обмена. Если строки ещё нет (объект пришёл в пакете раньше, чем его
        увидела sync) — вставляем.
        """
        table = self.objects_table
        with self.engine.begin() as conn:
            row = conn.execute(select(table.c.last_full_load_dt)
                               .where(table.c.object_name == object_name)).first()
            if row is None:
                conn.execute(insert(table).values(
                    object_name=object_name, object_type=object_name.split('_', 1)[0],
                    full_load_is_required=True, last_full_load_dt=None, is_deleted=False))
            elif row.last_full_load_dt is None:
                conn.execute(update(table).where(table.c.object_name == object_name)
                             .values(full_load_is_required=True))

    def list_full_load_required(self) -> list[str]:
        """Объекты, ожидающие полной выгрузки (требуется и не удалён из метаданных)."""
        table = self.objects_table
        with self.engine.connect() as conn:
            return list(conn.execute(
                select(table.c.object_name)
                .where(table.c.full_load_is_required, ~table.c[IS_DELETED_FIELD])).scalars())

    def mark_full_loaded(self, object_name: str) -> None:
        """Фиксирует успешную полную выгрузку: ставит last_full_load_dt=now(), снимает требование."""
        table = self.objects_table
        with self.engine.begin() as conn:
            conn.execute(update(table).where(table.c.object_name == object_name)
                         .values(last_full_load_dt=func.now(), full_load_is_required=False))