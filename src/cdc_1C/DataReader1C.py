import requests
import logging
from collections import UserDict, UserList

import xmltodict
import polars as pl

from cdc_1C.MetadataReader1C import MetadataReader1C

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGISTER_TYPES = ('InformationRegister','AccumulationRegister')
ENTITY_TYPES = ('Catalog','Document')
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'

POLARS_TYPE_MAPPING = {
    'Guid':     pl.Utf8,
    'String':   pl.Utf8,
    'Int64':    pl.Int64,
    'Int16':    pl.Int16,
    'Double':   pl.Float64,
    'Boolean':  pl.Boolean,
    'DateTime': pl.Datetime,
}


class DataObject1C(UserList):
    def __init__(self, records: list = [], metadata_obj=None):
        super().__init__(records)
        self.metadata_obj = metadata_obj  # MetadataObject1C или None

    def _get_polars_schema(self, columns: list[str]) -> dict:
        metadata_dict = self.metadata_obj or {}
        return {col: POLARS_TYPE_MAPPING.get(metadata_dict.get(col, 'String'), pl.Utf8)
                for col in columns}

    def to_dataframe(self, name_mapper=None) -> pl.DataFrame:
        columns = list(self[0].keys()) if self else list(self.metadata_obj or {})
        schema = self._get_polars_schema(columns)
        df = pl.DataFrame(list(self), schema=schema)
        if name_mapper:
            df = df.rename({col: name_mapper.map_field_name(col) for col in df.columns})
        return df


class DataReader1C(UserDict):
    def __init__(self, base_url: str, metadata: MetadataReader1C):
        super().__init__()
        self.base_url = base_url
        self.metadata = metadata

    def read_object(self, object_name: str):
        url = f"{self.base_url}/{object_name}"
        response = requests.get(url, auth=('admin', 'admin'))

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

    def _get_polars_schema(self, object_name: str) -> dict:
        metadata_obj = self.metadata.get(object_name) or {}
        return {name: POLARS_TYPE_MAPPING.get(type_str, pl.Utf8)
                for name, type_str in metadata_obj.items()}

    def _add_records(self, object_name, new_records: list):
        if new_records:
            if object_name in self.keys():
                self[object_name].extend(new_records)
            else:
                schema = self._get_polars_schema(object_name)
                self[object_name] = DataObject1C(new_records, schema=schema)

    @staticmethod
    def _get_record_fields(properties: dict) -> dict:
        # обычные поля; любой dict-значение нормализуем в None
        fields = {}
        for k, v in properties.items():
            if not k.startswith('d:') or k.endswith('_Type'):
                continue
            if isinstance(v, dict):
                fields[k.removeprefix('d:')] = None
            else:
                fields[k.removeprefix('d:')] = v

        # поля составных типов (убираем ODATA_PREFIX в значении)
        fields_type = {k.removeprefix('d:'): (v.removeprefix(ODATA_PREFIX) if isinstance(v, str) else v)
                       for k, v in properties.items()
                       if k.startswith('d:') and k.endswith('_Type') and not isinstance(v, dict)}

        return fields | fields_type

    def _get_register_records(self, object_name: str, properties: dict):
        """
        Функция забирает записи регистра, которые приходят по одному регистратору. Записей может быть несколько.
        Текущий объект это словарь, в котором ключом является объект 1С, например "Document_ЗаказКлиента",
        значения содержат массив записей в виде list of dict.
        """
        recorder = properties.get('d:Recorder')
        recorder_type = properties.get('d:Recorder_Type')

        records = (properties.get('d:RecordSet') or {}).get('d:element') or []

        new_records = [self._get_record_fields(record) for record in records]

        if not new_records:
            # Если записей нет, то значит запись удалена, создаем пустую запись.
            if recorder and recorder_type:
                recorder_name, _ = self._parse_object_full_name(recorder_type)
                new_records = [{'Recorder': recorder, 'Recorder_Type': recorder_name}]
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
        fields = self._get_record_fields(properties)
        self._add_records(object_name, [fields])

        table_parts = self._get_record_table_parts(properties)

        for table_part_key, table_part in table_parts.items():
            table_part_full_name = table_part.get('@m:type')
            if table_part_full_name:
                table_part_name, _ = self._parse_object_full_name(table_part_full_name)
                table_part_rows = table_part.get('d:element') or []
                if table_part_rows:
                    for table_part_row in table_part_rows:
                        self._add_records(table_part_name, [self._get_record_fields(table_part_row)])

    def to_dataframes(self, name_mapper=None) -> dict[str, pl.DataFrame]:
        result = {}
        for object_name in self.keys():
            mapped_name = name_mapper.map_object_name(object_name) if name_mapper else object_name
            result[mapped_name] = self[object_name].to_dataframe(name_mapper)
        return result
