import requests
import threading
from typing import Any
from collections import UserDict

import xmltodict
from sqlalchemy import (String, Uuid, BigInteger, Integer, SmallInteger, Numeric, Boolean, DateTime,
                        JSON, Engine, func, insert, select, update)
from sqlalchemy.dialects.postgresql import JSONB
from dbmerge import dbmerge

from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.common_functions import format_bytes, parse_object_full_name, raise_for_status
from cdc_1c.logging_config import get_logger, load_mode, LOAD_MODE_METADATA

logger = get_logger(__name__)

# Таймауты HTTP-запросов к 1С по умолчанию: (connect, read) в секундах. requests с timeout=None
# висит бесконечно при недоступном сервере.
# connect ограничивает ожидание установки соединения, read — ожидание ответа.
# Read — 15 минут: пакет изменений или страница полной выгрузки бывает реально большой,
# и 1С формирует его долго; таймаут должен ловить зависший сервер, а не медленный ответ.
# С периодом опроса это не связано: цикл опроса может быть заметно короче обработки пакета.
# Применяется, когда request_timeout не задан явно (None).
DEFAULT_REQUEST_TIMEOUT: tuple[float, float] = (60, 900)


def resolve_timeout(request_timeout: float | tuple[float, float] | None):
    """request_timeout как есть, либо default, если он не задан (None).
    Гарантирует, что ни один HTTP-запрос не уходит в requests с timeout=None (вечное ожидание)."""
    return DEFAULT_REQUEST_TIMEOUT if request_timeout is None else request_timeout


# Таблица-реестр объектов 1С и состояния их полной выгрузки (см. MetadataReader1C).
METADATA_OBJECTS_TABLE = 'metadata_objects_1c'

type_mapping = {'Guid':Uuid(),
                'Int64':BigInteger(),
                # Int32 приходит там, где у объекта числовой код/номер (Code, Number) и у номеров
                # сообщений плана обмена (ReceivedNo/SentNo) — см. Приложение 12 руководства
                # разработчика. Без него поле отбрасывалось бы как «неизвестный тип».
                'Int32':Integer(),
                'Int16':SmallInteger(),
                'String':String(),
                'Double':Numeric(),
                'Boolean':Boolean(),
                'DateTime':DateTime(),
                # ХранилищеЗначения публикуется парой полей: <Имя> (Edm.Stream, см. IGNORED_TYPES)
                # и <Имя>_Base64Data (Edm.Binary). Binary реально приходит в теле ответа
                # base64-строкой (в ней бывает JSON или XML-сериализация 1С), поэтому храним как текст.
                'Binary':String()}

# Типы, которых нет в данных: их нельзя отобразить в колонку, но это не ошибка метаданных.
# Collection — табличная часть, читается отдельным объектом.
# Stream — media-link (ХранилищеЗначения): в m:properties не приходит никогда,
# значение доступно только отдельным GET по ссылке.
IGNORED_TYPES = ('Collection', 'Stream')

GUESS_UUID_TYPES = True
# Проблема в том, что 1С часть GUID полей присылает как строки в описании метаданных.
# В этом модуле есть логика, которая определяет тип UUID поля, по имени поля "Recorder" 
# или по наличию другого поля с постфиксом "_Type" для составных типов данных.
# На всякий случай сделан этот флаг, чтобы можно было эту логику отключить.
# Конечно, если отключить флаг, то это часть полей будут UUID, а часть VARCHAR.
# В этом случает VARCHAR поля лучше руками в базе поменять на UUID, 
# т.к. иначе будут медленно работать JOIN

REGISTER_TYPES = ('InformationRegister','AccumulationRegister')
# Ссылочные классы: устроены одинаково (Ref + DeletionMark + реквизиты + табличные части),
# поэтому разбираются общим кодом. Регистры бухгалтерии и расчёта сюда не входят: у них своя
# структура записи (Dr/Cr-пары, счета, периоды действия) — см. Приложение 12 руководства
# разработчика, разделы 12.11 и 12.12.
ENTITY_TYPES = ('Catalog','Document','ChartOfCharacteristicTypes','ChartOfAccounts',
                'ChartOfCalculationTypes','BusinessProcess','Task')
# Классы, которые мы умеем сохранять. Всё остальное, придя в пакете изменений, будет потеряно
# (пакет подтверждается целиком), поэтому такие объекты логируются отдельно — см. read_data_entries.
SUPPORTED_TYPES = REGISTER_TYPES + ENTITY_TYPES
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'
TYPE_PREFIX = 'Edm.'

# Поля регистратора в OData: Recorder (+Recorder_Type), если регистратором может быть несколько
# типов документов, и Recorder_Key (Guid, без Recorder_Type), если тип регистратора единственный.
RECORDER_FIELDS = ('Recorder', 'Recorder_Key', 'Recorder_Type')
# Системные поля движений регистра (не измерения/ресурсы/реквизиты).
SYSTEM_REGISTER_FIELDS = frozenset(
    ('Period', 'LineNumber', 'Active', 'RecordType') + RECORDER_FIELDS)
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
    (нет ни _Balance, ни _Turnover) — ([], [], []). Регистр может иметь и остатки, и обороты,
    поэтому обе таблицы обрабатываются независимо.
    """
    prop_names = set(properties)
    balance = complextypes.get(base_name + '_Balance')
    turnover = complextypes.get(base_name + '_Turnover')

    if balance is None and turnover is None:
        return [], [], []

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
        # (require_full_load_if_new). Состав реестра синхронизируется с $metadata через dbmerge
        # (delete: пропавшие объекты удаляются, merged_on ведёт dbmerge).
        # Таблицу создаёт сам dbmerge при первой sync; objects_table — её Table-описание оттуда же.
        self.engine = engine
        self.schema = None if (engine is not None and engine.dialect.name == 'sqlite') else schema
        self.objects_table = None


    def _read_metadata_item_properties(self, item:dict, item_name: str | None = None):
        """
        Читаем поля объекта метаданных
        """
        item_name = item_name or item.get('@Name')
        item_properties = item.get('Property') or []

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
            elif property_type.startswith(IGNORED_TYPES):
                # ожидаемо не отображается в колонку (табличная часть / media-link), не ошибка
                logger.debug(f'Property {item_name}.{property_name} of type {property_type} '
                             f'is not stored as a column')
            else:
                logger.error(f'Property {item_name}.{property_name} has unknown type {property_type}')
            
        if GUESS_UUID_TYPES:
            for property_name in properties.keys():
                # Если мы видим что есть поле с постфиксом Type, то значит
                # ищем такое же поле без постфикса, т.к. в этом случае это составной тип и нужно
                # изменить поле на Uuid т.к. 1С почему-то присылает String
                if property_name.endswith('_Type'):
                    uuid_property_name = property_name.removesuffix('_Type')
                    if uuid_property_name in properties.keys():
                        properties[uuid_property_name] = 'Guid'


        return properties

    def _read_metadata_item_key(self, item:dict, properties: dict):
        """
        Читаем список ключевых полей объекта метаданных
        """
        item_key = (item.get('Key') or {}).get('PropertyRef')
        # Ключа может не быть (например, у сущности без объявленного Key) — тогда пустой список,
        # иначе обращение к key_fields ниже упало бы с UnboundLocalError.
        key_fields = ([k.get('@Name') for k in item_key if k.get('@Name') is not None]
                      if item_key else [])

        return {k: properties[k] for k in key_fields if k in properties}


    def _get_object_key(self, item_name: str, properties: dict, primary_key: dict):
        """
        Ключ для scoped-удаления при merge (delete_condition в dbmerge).
        Изменения приходят группами, которые целиком заменяют существующие строки:
        - регистр: набор записей одного регистратора -> Recorder (+ Recorder_Type) либо
          Recorder_Key, смотря как 1С назвала поле регистратора (см. RECORDER_FIELDS);
        - табличная часть (ключ Ref_Key + ещё поля) -> Ref_Key владельца;
        - документ/справочник (единственная запись по Ref_Key) -> None, удаление не нужно.
        """
        if item_name.startswith(REGISTER_TYPES):
            return [c for c in RECORDER_FIELDS if c in properties]
        if 'Ref_Key' in primary_key and len(primary_key) > 1:
            return ['Ref_Key']
        return None

    def get_metadata(self):
        """
        Запрашиваем метаданные всех доступных объектов из odata и (если задан engine) синхронизируем
        реестр metadata_objects_1c с актуальным составом $metadata.
        Можно вызывать повторно для обновления (при появлении нового объекта/поля — см. data_reader);
        под блокировкой, т.к. вызывается и из фоновых потоков full_load. В конце is_loaded=True.

        В логе помечается своим режимом (METADATA), а не режимом вызвавшей операции: чтение
        $metadata общее для пакета изменений и полной выгрузки, и метка CHANGES на нём вводила бы
        в заблуждение.
        """
        with self._lock, load_mode(LOAD_MODE_METADATA):
            self._fetch_and_parse_metadata()
            self.is_loaded = True
            if self.engine is not None:
                self._sync_objects(list(self.keys()))

    def _fetch_and_parse_metadata(self):
        logger.info('Requesting metadata from 1C ODATA')
        
        url = f'{self.odata_url}/$metadata'
        response = requests.get(url,auth=self.odata_auth,
                                timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, '$metadata')
        logger.info('Metadata received (%s)', format_bytes(len(response.content)))

        metadata = xmltodict.parse(response.text,force_list=('Property','PropertyRef','ComplexType'))
        metadata_schema = ((metadata.get('edmx:Edmx') or {}).get('edmx:DataServices') or {}).get('Schema') or {}
        metadata_entity_types = metadata_schema.get('EntityType') or []
        # Виртуальные таблицы регистров (_Balance/_Turnover) приходят как ComplexType — нужны для
        # классификации полей регистра на измерения/ресурсы/реквизиты (см. _classify_register_fields).
        # Табличные части документов и справочников также приходят как ComplexType.
        complextypes = {ct.get('@Name'): [p.get('@Name') for p in (ct.get('Property') or [])]
                        for ct in (metadata_schema.get('ComplexType') or [])}


        for item in metadata_entity_types:

            item_name = item.get('@Name')

            if item_name.startswith(REGISTER_TYPES) and item_name.endswith("_RecordType"):
            # регистр с постфиксом RecordType содержит описание полей регистра и описание ключа
                item_name = item_name.removesuffix("_RecordType")
                properties = self._read_metadata_item_properties(item, item_name)
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
                properties = self._read_metadata_item_properties(item, item_name)
                primary_key = self._read_metadata_item_key(item,properties)
                object_key = self._get_object_key(item_name, properties, primary_key)
                is_table_part = _check_object_is_table_part(item_name, complextypes)
                self[item_name] = MetadataObject1C(item_name, properties, primary_key, object_key, 
                                                   is_table_part=is_table_part)



    # --- Реестр объектов и состояния полной выгрузки (metadata_objects_1c) ---

    def _sync_objects(self, object_names: list[str]) -> None:
        """
        Синхронизирует реестр с актуальным составом $metadata через dbmerge (delete): новые объекты
        вставляются, пропавшие из метаданных — удаляются из таблицы реестра.
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
                # эти значения устанавливаются только при insert, из update они исключены
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
        признак членства в плане обмена. Ключ реестра — полное имя (object_full_name): регистр и
        документ могут иметь одинаковое короткое имя. Если строки ещё нет (объект пришёл в пакете
        раньше, чем его увидела sync) — перечитываем метаданные (sync заведёт строку) и помечаем.
        """
        def _last_full_load_dt():
            table = self.objects_table
            with self.engine.begin() as conn:
                return conn.execute(select(table.c.last_full_load_dt)
                                    .where(table.c.object_full_name == object_full_name)).first()

        row = _last_full_load_dt()
        if row is None:
            # Объект ещё не в реестре — перечитываем метаданные (get_metadata → _sync_objects
            # вставит строку). Вне транзакции: get_metadata сам открывает соединения через dbmerge.
            logger.info('Object %s not in registry yet, reloading metadata', object_full_name)
            self.get_metadata()
            row = _last_full_load_dt()

        if row is not None and row.last_full_load_dt is None:
            table = self.objects_table
            with self.engine.begin() as conn:
                conn.execute(update(table).where(table.c.object_full_name == object_full_name)
                             .values(full_load_is_required=True))

    def list_full_load_required(self) -> list[str]:
        """Полные имена объектов, ожидающих полной выгрузки (full_load_is_required)."""
        table = self.objects_table
        with self.engine.connect() as conn:
            return list(conn.execute(
                select(table.c.object_full_name)
                .where(table.c.full_load_is_required)).scalars())

    def mark_full_loaded(self, object_full_name: str) -> None:
        """Фиксирует успешную полную выгрузку: ставит last_full_load_dt=now(), снимает требование.
        Ключ — полное имя (object_full_name), а не короткое: имена регистра и документа могут совпасть."""
        table = self.objects_table
        with self.engine.begin() as conn:
            conn.execute(update(table).where(table.c.object_full_name == object_full_name)
                         .values(last_full_load_dt=func.now(), full_load_is_required=False))