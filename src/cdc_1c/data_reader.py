import requests
import logging
from typing import Any
from collections import UserDict, UserList
from datetime import datetime
import uuid

import xmltodict

from cdc_1c.metadata_reader import MetadataReader1C

logger = logging.getLogger(__name__)

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


class DataObject1C(UserDict):
    def __init__(self, metadata_obj=None, records: list = []):
        super().__init__()
        self.metadata_obj = metadata_obj  # MetadataObject1C
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

    def to_records_mapped(self, column_mapping: dict[str, str] | None = None) -> list[dict]:
        """
        Преобразует колоночное хранилище (dict of lists) в список записей (list of dict)
        для передачи в dbmerge. Если задан column_mapping, имена колонок заменяются на лету,
        без отдельной переименованной копии данных (значения колонок переиспользуются по ссылке).
        """
        keys = list(self.data.keys())
        out_keys = [column_mapping.get(k, k) for k in keys] if column_mapping else keys
        cols = list(self.data.values())
        return [dict(zip(out_keys, row)) for row in zip(*cols)]
            


class DataReader1C(UserDict):
    def __init__(self, base_url: str, metadata: MetadataReader1C,
                 auth: tuple[str, str] | None = None):
        super().__init__()
        self.base_url = base_url
        self.metadata = metadata
        self.auth = auth
        self.exchange_message_no = None  # номер пакета обмена, проставляется в записи при чтении изменений

    def read_object(self, object_name: str):
        url = f"{self.base_url}/{object_name}"
        response = requests.get(url, auth=self.auth)

        object_data = xmltodict.parse(response.text, force_list=('d:element', 'entry'))
        object_entries = (object_data.get('feed') or {}).get('entry') or []

        self.clear()
        self.read_data_entries(object_entries)

    def read_data_entries(self, object_entries: list):
        for object_entry in object_entries:

            object_full_name = (object_entry.get('category') or {}).get('@term')
            object_name, object_type = self._parse_object_full_name(object_full_name)

            logger.info(f'Parcing {object_name}')

            if object_name and self.metadata.get(object_name) is None:
                self.metadata.get_metadata()
                if self.metadata.get(object_name) is None:
                    logger.warning(f'Metadata not found for {object_name}')

            properties = (object_entry.get('content') or {}).get('m:properties') or {}

            if object_type in REGISTER_TYPES:
                self._get_register_records(object_name, properties)

            if object_type in ENTITY_TYPES:
                self._get_entity_records(object_name, properties)

    def _parse_object_full_name(self, object_full_name):
        """
        Очищаем имя объекта от разных префиксов, постфиксов и скобок.
        Возвращает очищенное имя и тип объекта
        """
        if object_full_name is None:
            logger.error(f'Object full name is None')
            return None, None

        object_name = object_full_name

        if object_name.startswith('Collection'):
            object_name = object_name.removeprefix('Collection(')
            object_name = object_name.removesuffix(')')

        object_name = object_name.removeprefix(ODATA_PREFIX)
        object_name = object_name.removesuffix('_RowType')

        if '_' in object_name:
            object_type = object_name.split('_')[0]
        else:
            logger.error(f'Object type not found in object full name {object_full_name}')
            return None, None
        return object_name, object_type

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

    def _get_record_fields(self, properties: dict, object_name: str = None) -> dict:
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
                    # поле не найдено в метаданных, пробуем их перечитать.
                    self.metadata.get_metadata()
                    metadata_obj = self.metadata[object_name]
                    if field_name not in metadata_obj.keys():
                        logger.warning(f'Metadata field {field_name} not found for object {object_name}')
                        
                type_name = metadata_obj.get(field_name) or 'String'

                converted = self._convert_value(value, type_name)

                fields[field_name] = converted

        # Спец-поле: для документов/справочников True по пометке удаления,
        # для регистров/табличных частей (нет DeletionMark) — False.
        fields[IS_DELETED_OR_EMPTY_FIELD] = bool(fields.get(DELETION_MARK_FIELD))
        fields[EXCHANGE_MESSAGE_NO_FIELD] = self.exchange_message_no

        return fields

    @staticmethod
    def _default_key_value(type_name: str) -> Any:
        # Дефолтное значение поля ключа для удаленной записи регистра:
        # '' для строк, 0 для чисел, чтобы получить непустой составной ключ.
        if type_name == 'String':
            return ''
        if type_name in ('Int64', 'Int16', 'Double'):
            return 0
        if type_name == 'Boolean':
            return False
        if type_name == 'DateTime':
            return datetime(1, 1, 1)  # пустая дата 1С
        return None  # Guid (Recorder перекрывается ниже) и прочее

    def _make_deleted_register_record(self, object_name: str, recorder: str, recorder_type: str) -> dict:
        """
        Создает запись для удаленного набора записей регистра (пришел пустой RecordSet).
        Заполняет полный первичный ключ из метаданных дефолтами по типу поля
        и проставляет реальные Recorder и Recorder_Type.
        """
        primary_key = self.metadata[object_name].primary_key
        record = {field: self._default_key_value(type_name)
                  for field, type_name in primary_key.items()}

        recorder_name, _ = self._parse_object_full_name(recorder_type)
        record['Recorder'] = recorder
        record['Recorder_Type'] = recorder_name
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
        Функция забирает записи регистра, которые приходят по одному регистратору. Записей может быть несколько.
        Текущий объект это словарь, в котором ключом является объект 1С, например "Document_ЗаказКлиента",
        значения содержат массив записей в виде list of dict.
        """
        recorder = properties.get('d:Recorder')
        recorder_type = properties.get('d:Recorder_Type')

        records = (properties.get('d:RecordSet') or {}).get('d:element') or []

        new_records = [self._get_record_fields(record, object_name) for record in records]

        if not new_records:
            # Если записей нет, то значит набор записей удален.
            if recorder and recorder_type:
                new_records = [self._make_deleted_register_record(object_name, recorder, recorder_type)]
            else:
                logger.error(f'No recorder or recorder type for {object_name}')

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
                table_part_name, _ = self._parse_object_full_name(table_part_full_name)
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
