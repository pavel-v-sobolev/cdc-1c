from __future__ import annotations

import requests
import logging
from typing import Any
from collections import UserDict
from datetime import datetime, date
from urllib.parse import quote
import uuid

import xmltodict

from cdc_1c.metadata_reader import MetadataReader1C, resolve_timeout
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.common_functions import parse_object_full_name, raise_for_status

logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
    """Приводит значение к JSON-сериализуемому виду: UUID -> str, datetime/date -> ISO."""
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value

REGISTER_TYPES = ('InformationRegister','AccumulationRegister')
ENTITY_TYPES = ('Catalog','Document')
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'

# Поле 1С с пометкой удаления у документов и справочников.
DELETION_MARK_FIELD = 'DeletionMark'
# Спец-поле, заполняемое при загрузке: True для удаленных объектов (DeletionMark)
# и для фиктивных записей (удаленный набор регистра, опустевшая табличная часть).
IS_DELETED_OR_EMPTY_FIELD = 'is_deleted_or_empty'
# Спец-поле: номер пакета обмена (message_no), проставляется во все записи при чтении изменений.
EXCHANGE_MESSAGE_NO_FIELD = 'exchange_message_no'

# Поля регистратора в OData. 1С называет их по-разному в зависимости от того, сколько типов
# документов может быть регистратором регистра:
# - несколько типов (составная ссылка) -> Recorder (строка) + Recorder_Type (имя типа);
# - единственный тип -> Recorder_Key (Guid), без Recorder_Type.
RECORDER_FIELDS = ('Recorder', 'Recorder_Key')
RECORDER_TYPE_FIELD = 'Recorder_Type'
# Набор записей регистра в entry регистраторного регистра (у независимого регистра его нет).
RECORD_SET_FIELD = 'd:RecordSet'
# Пустая ссылка 1С — нулевой GUID (в данных 1С отдаёт именно его).
EMPTY_UUID = uuid.UUID(int=0)


def _odata_literal(value: Any, type_name: str) -> str:
    """
    OData-литерал значения по типу поля (для keyset-фильтра): Guid → guid'…', DateTime → datetime'…',
    числа — как есть, Boolean → true/false, остальное (String и пр.) — строка в кавычках (кавычка
    внутри экранируется удвоением). value приходит уже сконвертированным (_convert_value): UUID/datetime.
    """
    if type_name == 'Guid':
        return f"guid'{value}'"
    if type_name == 'DateTime':
        v = value.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(value, (datetime, date)) else str(value)
        return f"datetime'{v}'"
    if type_name in ('Int64', 'Int16', 'Double'):
        return str(value)
    if type_name == 'Boolean':
        return 'true' if value else 'false'
    return "'" + str(value).replace("'", "''") + "'"


def _keyset_filter(key_fields: list[str], after_values: list, key_types: list[str]) -> str:
    """
    Лексикографический keyset-фильтр «строка ключа > последней строки предыдущей страницы» для
    составного ключа (key_fields, порядок = порядок $orderby):
        (k1 gt v1) or (k1 eq v1 and k2 gt v2) or (k1 eq v1 and k2 eq v2 and k3 gt v3) ...
    Для одиночного ключа сводится к «k1 gt v1». Литералы — по типам key_types (_odata_literal).
    """
    terms = []
    for i in range(len(key_fields)):
        conj = [f"{key_fields[j]} eq {_odata_literal(after_values[j], key_types[j])}" for j in range(i)]
        conj.append(f"{key_fields[i]} gt {_odata_literal(after_values[i], key_types[i])}")
        terms.append(" and ".join(conj))
    return " or ".join(f"({t})" if " and " in t else t for t in terms)


class DataObject1C(UserDict):
    def __init__(self, metadata_obj=None, records: list = []):
        super().__init__()
        self.metadata_obj = metadata_obj  # MetadataObject1C
        # Табличные части этого объекта: {имя_части: DataObject1C}. Заполняются при чтении
        # (_get_entity_records связывает владельца с его ТЧ), используются to_nested_records.
        self.table_parts: dict[str, DataObject1C] = {}
        self.data_length = 0
        self.add_records(records)

    def add_records(self, records: list):
        for record in records:
            for k,v in record.items():
                if k not in self.data.keys():
                    # встретилось новое поле, нужно добавить его во все записи, которые уже есть.
                    if self.data_length > 0:
                        self.data[k] = [None] * self.data_length
                    else:
                        self.data[k] = []
                self.data[k].append(v)

            for k in self.data.keys():
                 # если в новой записи нет какого-то поля, которое уже есть в данных, то нужно добавить это поле со значением None
                if k not in record.keys():
                    self.data[k].append(None)

            self.data_length += 1

    def to_records_mapped(self, column_mapping: dict[str, str] | None = None,
                          json_safe: bool = False) -> list[dict]:
        """
        Преобразует колоночное хранилище (dict of lists) в список записей (list of dict)
        для передачи в dbmerge. Если задан column_mapping, имена колонок заменяются на лету,
        без отдельной переименованной копии данных (значения колонок переиспользуются по ссылке).

        json_safe=True приводит значения к JSON-сериализуемому виду (UUID->str, datetime->ISO) —
        для экспорта в JSON; по умолчанию False, чтобы dbmerge получал исходные типы.
        """
        keys = list(self.data.keys())
        out_keys = [column_mapping.get(k, k) for k in keys] if column_mapping else keys
        cols = list(self.data.values())
        if json_safe:
            return [dict(zip(out_keys, (_json_safe(v) for v in row))) for row in zip(*cols)]
        return [dict(zip(out_keys, row)) for row in zip(*cols)]

    def group_by(self, key_field: str = 'Ref_Key', name_mapper: NameMapper1C | None = None,
                 json_safe: bool = False, skip_deleted: bool = False) -> dict[Any, list[dict]]:
        """
        Группирует записи объекта в {значение key_field: [записи]} одним проходом (hash group-by).
        Записи — как в to_records_mapped (с маппингом/json_safe). Индекс не хранится в объекте, а
        строится на вызов: платим только при экспорте, без накладных в add_records и без устаревания.

        skip_deleted=True пропускает записи с is_deleted_or_empty (удалённые/фиктивные строки) —
        для вложенных табличных частей: опустевшая ТЧ приходит фиктивной записью и должна дать
        пустой список, а не группу с записью-пустышкой.
        """
        col_map = name_mapper.get_column_mapping(list(self.data.keys())) if name_mapper else None
        key = name_mapper.map_field_name(key_field) if name_mapper else key_field
        del_key = (name_mapper.map_field_name(IS_DELETED_OR_EMPTY_FIELD)
                   if name_mapper else IS_DELETED_OR_EMPTY_FIELD)
        grouped: dict[Any, list[dict]] = {}
        for row in self.to_records_mapped(col_map, json_safe=json_safe):
            if skip_deleted and row.get(del_key):
                continue
            grouped.setdefault(row.get(key), []).append(row)
        return grouped

    def to_nested_records(self, name_mapper: NameMapper1C | None = None,
                          json_safe: bool = False) -> list[dict]:
        """
        Записи этого объекта (list of dict) с вложенными табличными частями — например, чтобы
        отправить во внешний приёмник (RabbitMQ и т.п.) вместо записи в БД.

        Табличные части берутся из self.table_parts (их связал контейнер при чтении), кладутся
        вложенным списком под ключом = имя части, строки группируются по Ref_Key (group_by).

        name_mapper=None → имена полей/частей как в 1С; передан — транслитерируем (как в БД).
        json_safe=True → значения JSON-сериализуемы (UUID->str, datetime->ISO), готово к json.dumps.
        """
        col_map = name_mapper.get_column_mapping(list(self.data.keys())) if name_mapper else None
        key = name_mapper.map_field_name('Ref_Key') if name_mapper else 'Ref_Key'
        records = self.to_records_mapped(col_map, json_safe=json_safe)

        for part_name, part_obj in self.table_parts.items():
            part_key = name_mapper.map_field_name(part_name) if name_mapper else part_name
            grouped = part_obj.group_by('Ref_Key', name_mapper, json_safe, skip_deleted=True)
            for rec in records:
                rec[part_key] = grouped.get(rec.get(key), [])
        return records
            


class DataReader1C(UserDict):
    def __init__(self, odata_url: str, metadata: MetadataReader1C,
                 odata_auth: tuple[str, str] | None = None,
                 request_timeout: float | None = None):
        super().__init__()
        self.odata_url = odata_url
        self.metadata = metadata
        self.odata_auth = odata_auth
        self.request_timeout = request_timeout
        self.exchange_message_no = None  # номер пакета обмена, проставляется в записи при чтении изменений
        # Размер последнего ответа 1С в байтах — по нему вызывающий подбирает размер страницы
        # (вес одной entry у разных объектов различается на порядки, см. Replicator1C.full_load).
        self.last_response_bytes = 0
        # Объекты, ради неизвестного поля которых метаданные уже перечитывались (см. _get_record_fields).
        # Без этого каждая запись с полем вне $metadata давала бы свой полный GET $metadata + dbmerge.
        self._metadata_refreshed_for: set[str] = set()

    def read_object(self, object_name: str, top: int | None = None,
                    key_fields: list[str] | None = None, after_values: list | None = None,
                    key_types: list[str] | None = None, extra_filter: str | None = None,
                    skip: int | None = None) -> int:
        """
        Читает объект 1С в reader (предыдущее содержимое очищается). Страница — сортировка по ключу
        ($orderby key_fields) и лимит $top; следующая страница берётся одним из двух способов:

        - keyset (after_values): лексикографический фильтр «ключ больше последней строки предыдущей
          страницы». Не заставляет 1С перечитывать пропущенные строки, но применим, только если в
          $orderby нет ссылочных полей (см. skip);
        - $skip: смещение от начала выборки. 1С перечитывает пропущенные строки, зато работает для
          любого ключа.

        skip и after_values взаимоисключающи; способ выбирает вызывающий (см. Replicator1C.full_load).

        key_fields/key_types (порядок = порядок сортировки; типы — для литералов, см. _odata_literal):
        - справочник/документ: ['Ref_Key'] / ['Guid'];
        - регистраторный регистр: ['Recorder'] / ['String'] либо ['Recorder_Key'] / ['Guid'] —
          одна entry = целый набор записей регистратора, поэтому страница не рвёт набор;
        - независимый регистр (нет Ref_Key/Recorder): весь первичный ключ (Period + измерения) —
          составной ключ, т.к. одиночного уникального курсора нет.

        extra_filter — дополнительный OData-фрагмент $filter (например, диапазон по дате), который
        объединяется с keyset-условием по AND (составной keyset содержит OR — оборачиваем в скобки).

        Возвращает число прочитанных записей верхнего уровня (entry) — по нему вызывающий понимает,
        что страница последняя (меньше top).
        """
        key_fields = key_fields or ['Ref_Key']
        key_types = key_types or ['Guid'] * len(key_fields)
        params = []
        if top is not None:
            params.append(f"$top={top}")
        if skip:
            params.append(f"$skip={skip}")
        params.append("$orderby=" + ','.join(key_fields))
        # keyset-курсор и extra_filter объединяем по AND в один $filter.
        filters = []
        if after_values is not None:
            keyset = _keyset_filter(key_fields, after_values, key_types)
            # составной keyset содержит верхнеуровневый OR — оборачиваем в скобки, чтобы AND с
            # extra_filter не исказил приоритет; одиночный ключ (без OR) оставляем как есть.
            filters.append(f"({keyset})" if ' or ' in keyset else keyset)
        if extra_filter:
            filters.append(extra_filter)
        if filters:
            # URL собираем строкой — кодируем пробелы сами (requests строку не кодирует); двоеточия
            # в datetime-литералах оставляем как есть (safe).
            params.append("$filter=" + quote(" and ".join(filters), safe="':"))
        query = '?' + '&'.join(params)
        url = f"{self.odata_url}/{object_name}{query}"
        response = requests.get(url, auth=self.odata_auth, timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, f'read {object_name}{query}')
        self.last_response_bytes = len(response.content)

        object_data = xmltodict.parse(response.text, force_list=('d:element', 'entry'))
        object_entries = (object_data.get('feed') or {}).get('entry') or []

        self.clear()
        self.read_data_entries(object_entries)
        return len(object_entries)

    def read_data_entries(self, object_entries: list):
        for object_entry in object_entries:

            object_full_name = (object_entry.get('category') or {}).get('@term')
            object_name, object_type = parse_object_full_name(object_full_name)

            logger.info(f'Parsing {object_name}')

            if object_name and self.metadata.get(object_name) is None:
                self.metadata.get_metadata()
                if self.metadata.get(object_name) is None:
                    logger.warning(f'Metadata not found for {object_name}')

            properties = (object_entry.get('content') or {}).get('m:properties') or {}

            if object_type in REGISTER_TYPES:
                self._get_register_records(object_name, properties)

            if object_type in ENTITY_TYPES:
                self._get_entity_records(object_name, properties)


    def _add_records(self, object_name, new_records: list):
        if new_records:
            if object_name in self.keys():
                self[object_name].add_records(new_records)
            else:
                metadata_obj = self.metadata.get(object_name)
                self[object_name] = DataObject1C(metadata_obj=metadata_obj, records=new_records)

    @staticmethod
    def _convert_value(value: Any, type_name: str) -> Any:
        # Конвертируем значение в нужный тип данных на основе метаданных.
        if value is None:
            return None
        if not isinstance(value, str):
            logger.warning(f'Expected value to be a string for conversion, got {type(value).__name__}: {value!r}')
            return value
        try:
            if type_name == 'Boolean':
                return value.lower() == 'true'
            if type_name in ('Int64', 'Int16'):
                return int(value)
            if type_name == 'Double':
                return float(value)
            if type_name == 'DateTime':
                return datetime.fromisoformat(value)
            if type_name == 'Guid':
                if value == '00000000-0000-0000-0000-000000000000':
                    return None
                else:
                    return uuid.UUID(value)
        except (ValueError, TypeError) as e:
            logger.warning(f'Failed to convert value {value!r} to type {type_name}: {e}')
        return value

    def _get_record_fields(self, properties: dict, object_name: str | None = None) -> dict:
        if object_name not in self.metadata.keys():
            self.metadata.get_metadata()
            if object_name not in self.metadata.keys():
                logger.error(f'Metadata not found for {object_name}')
                raise ValueError(f'Metadata not found for {object_name}')            
        
        metadata_obj = self.metadata[object_name]

        fields = {}
        for k, v in properties.items():
            if k.startswith('d:') and isinstance(v, str):

                field_name = k.removeprefix('d:')

                if field_name.endswith('_Type'):
                    # если поле заканчивается на _Type, то это поле составного типа
                    # и в значении будет полное имя типа, например "StandardODATA.СправочникСсылка.Контрагенты"
                    # для удобства убираем префикс и оставляем только имя типа, например "СправочникСсылка.Контрагенты"
                    value = v.removeprefix(ODATA_PREFIX)
                else:
                    value = v

                if field_name not in metadata_obj.keys():
                    # поле не найдено в метаданных, пробуем их перечитать — но не более одного раза
                    # на объект, иначе поле, которого в $metadata нет вовсе (например, отброшенный
                    # по неизвестному типу реквизит), даёт по запросу метаданных на каждую запись.
                    if object_name not in self._metadata_refreshed_for:
                        self._metadata_refreshed_for.add(object_name)
                        self.metadata.get_metadata()
                        metadata_obj = self.metadata[object_name]
                    if field_name not in metadata_obj.keys():
                        logger.warning(f'Metadata field {field_name} not found for object {object_name}')


                type_name = metadata_obj.get(field_name) or 'String'

                converted = self._convert_value(value, type_name)

                if converted is None and field_name in metadata_obj.primary_key:
                    # Поля первичного ключа в целевой таблице NOT NULL, а пустую ссылку 1С
                    # (нулевой GUID) _convert_value превращает в None — для ключа оставляем
                    # дефолт по типу (нулевой GUID), иначе строка не вставится.
                    converted = self._default_key_value(type_name)

                fields[field_name] = converted

        # Спец-поле: для документов/справочников True по пометке удаления,
        # для регистров/табличных частей (нет DeletionMark) — False.
        fields[IS_DELETED_OR_EMPTY_FIELD] = bool(fields.get(DELETION_MARK_FIELD))
        fields[EXCHANGE_MESSAGE_NO_FIELD] = self.exchange_message_no

        return fields

    @staticmethod
    def _default_key_value(type_name: str) -> Any:
        # Дефолтное значение поля ключа (удаленная запись регистра, пустая ссылка в ключе):
        # '' для строк, 0 для чисел, нулевой GUID для ссылок — чтобы получить непустой
        # составной ключ (в целевой таблице поля ключа NOT NULL).
        if type_name == 'String':
            return ''
        if type_name in ('Int64', 'Int16', 'Double'):
            return 0
        if type_name == 'Boolean':
            return False
        if type_name == 'DateTime':
            return datetime(1, 1, 1)  # пустая дата 1С
        if type_name == 'Guid':
            return EMPTY_UUID  # пустая ссылка 1С
        return None

    def _entry_recorder_fields(self, object_name: str, properties: dict) -> dict:
        """
        Поля регистратора из entry набора записей: Recorder + Recorder_Type (составной
        регистратор) либо Recorder_Key (единственный тип регистратора, см. RECORDER_FIELDS).

        Ключ набора лежит на уровне entry, и внутри строк набора 1С его не повторяет (хотя в
        $metadata он объявлен и в _RowType) — поэтому значения берём отсюда и проставляем в
        каждую строку набора, как Ref_Key владельца в строки табличной части.
        """
        metadata_obj = self.metadata[object_name]
        fields = {}
        for field in RECORDER_FIELDS:
            value = properties.get('d:' + field)
            if isinstance(value, str):
                fields[field] = self._convert_value(value, metadata_obj.get(field) or 'Guid')

        recorder_type = properties.get('d:' + RECORDER_TYPE_FIELD)
        if isinstance(recorder_type, str):
            fields[RECORDER_TYPE_FIELD], _ = parse_object_full_name(recorder_type)
        return fields

    def _make_deleted_register_record(self, object_name: str, properties: dict) -> dict | None:
        """
        Создает запись для удаленного набора записей регистра (пришел пустой RecordSet).
        Заполняет полный первичный ключ из метаданных дефолтами по типу поля и проставляет
        реальные поля регистратора из entry (_entry_recorder_fields).

        Возвращает None, если полей регистратора в entry нет — тогда непонятно, чей набор
        удалять, и фиктивную запись создавать нельзя (ключ остался бы пустым).
        """
        metadata_obj = self.metadata[object_name]
        record = {field: self._default_key_value(type_name)
                  for field, type_name in metadata_obj.primary_key.items()}

        recorder_fields = self._entry_recorder_fields(object_name, properties)
        if not any(field in recorder_fields for field in RECORDER_FIELDS):
            logger.error(f'No recorder field in empty record set of {object_name}')
            return None
        record.update(recorder_fields)

        record[IS_DELETED_OR_EMPTY_FIELD] = True
        record[EXCHANGE_MESSAGE_NO_FIELD] = self.exchange_message_no
        return record

    def _make_empty_table_part_record(self, table_part_name: str, ref_key: Any) -> dict:
        """
        Создает запись для опустевшей табличной части (пришла без строк), по аналогии с удаленным
        набором регистра. Полный ключ из метаданных заполняется дефолтами, а Ref_Key —
        реальным значением владельца, чтобы scoped-удаление по Ref_Key убрало старые строки.
        """
        primary_key = self.metadata[table_part_name].primary_key
        record = {field: self._default_key_value(type_name)
                  for field, type_name in primary_key.items()}
        record['Ref_Key'] = ref_key
        record[IS_DELETED_OR_EMPTY_FIELD] = True
        record[EXCHANGE_MESSAGE_NO_FIELD] = self.exchange_message_no
        return record

    def _get_register_records(self, object_name: str, properties: dict):
        """
        Функция забирает записи регистра.

        Регистраторный регистр приходит набором по одному регистратору (d:RecordSet с движениями);
        пустой набор = набор удалён (фиктивная запись). Независимый регистр сведений приходит плоско —
        поля прямо в properties, без RecordSet, как у справочника/документа (форма прямого
        чтения GET /InformationRegister_X).

        Признак регистраторного регистра — наличие RecordSet, а не Recorder: поле регистратора 1С
        называет то Recorder (+Recorder_Type), то Recorder_Key — см. RECORDER_FIELDS.

        Текущий объект это словарь, в котором ключом является объект 1С, например "Document_ЗаказКлиента",
        значения содержат массив записей в виде list of dict.
        """
        if RECORD_SET_FIELD not in properties:
            # Независимый регистр сведений: одна плоская запись, поля прямо в properties.
            self._add_records(object_name, [self._get_record_fields(properties, object_name)])
            return

        record_set = properties.get(RECORD_SET_FIELD)
        records = (record_set.get('d:element') or []) if isinstance(record_set, dict) else []

        if records:
            # Регистратор лежит на уровне entry и внутри строк набора не повторяется —
            # проставляем его в каждую строку (setdefault: если 1С всё же прислала, не трогаем).
            recorder_fields = self._entry_recorder_fields(object_name, properties)
            new_records = []
            for record in records:
                row = self._get_record_fields(record, object_name)
                for field, value in recorder_fields.items():
                    row.setdefault(field, value)
                new_records.append(row)
        else:
            # Регистраторный регистр с пустым набором — набор записей удалён.
            deleted_record = self._make_deleted_register_record(object_name, properties)
            new_records = [deleted_record] if deleted_record is not None else []

        self._add_records(object_name, new_records)

    def _get_record_table_parts(self, properties):
        """
        Ищем табличные части в свойствах объекта.
        Если тип данных dict и если префикс в названии 'd:', то будем считать что это табличная часть
        """
        table_parts = {k.removeprefix('d:'): v for k, v in properties.items()
                       if k.startswith('d:') and isinstance(v, dict) and v.get('@xsi:nil') != 'true'}
        return table_parts

    def _get_entity_records(self, object_name: str, properties: dict):
        """
        Функция забирает поля документа или справочника.
        Запись одна, но могут быть табличные части, которые будут записаны в отдельные элементы структуры changes
        """
        fields = self._get_record_fields(properties, object_name)
        self._add_records(object_name, [fields])

        # Строки табличных частей приходят без ссылки на владельца — проставляем Ref_Key документа.
        ref_key = fields.get('Ref_Key')
        # Пометку удаления документа/справочника распространяем на его табличные части.
        parent_deleted = fields.get(IS_DELETED_OR_EMPTY_FIELD)

        table_parts = self._get_record_table_parts(properties)

        for table_part_key, table_part in table_parts.items():
            table_part_full_name = table_part.get('@m:type')
            if table_part_full_name:
                table_part_name, _ = parse_object_full_name(table_part_full_name)
                table_part_rows = table_part.get('d:element') or []
                if table_part_rows:
                    for table_part_row in table_part_rows:
                        row = self._get_record_fields(table_part_row, table_part_name)
                        row['Ref_Key'] = ref_key
                        row[IS_DELETED_OR_EMPTY_FIELD] = parent_deleted
                        self._add_records(table_part_name, [row])
                else:
                    # Табличная часть пришла без строк — добавляем фиктивную запись,
                    # чтобы scoped-удаление по Ref_Key убрало ранее сохраненные строки.
                    ref_key = fields.get('Ref_Key')
                    if ref_key is not None and table_part_name in self.metadata.keys():
                        self._add_records(table_part_name,
                                          [self._make_empty_table_part_record(table_part_name, ref_key)])

                # Связываем объект-ТЧ с владельцем — чтобы to_nested_records нашёл его сам.
                if table_part_name in self:
                    self[object_name].table_parts[table_part_key] = self[table_part_name]
