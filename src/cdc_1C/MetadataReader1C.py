import requests
import logging
from typing import Any
from collections import UserDict

import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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




class MetadataObject1C(UserDict):
    def __init__(self,properties,primary_key):
        super().__init__(properties)
        self.primary_key = primary_key

    def get_column_types(self) -> dict[str, Any]:
        return {col: type_mapping[typ] for col, typ in self.data.items()}

class MetadataReader1C(UserDict):
    def __init__(self, base_url:str):
        super().__init__()
        self.base_url=base_url
        self.get_metadata()

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


    def get_metadata(self):
        """
        Запрашиваем метаданные всех доступных объектов из odata.
        Далее в цикле читаем из содержимое и сохраняем в структуре, которую будем использовать дальше,
        при создании таблиц и полей.
        """
        logger.info('Requesting metadata from 1C ODATA')
        
        url = f'{self.base_url}/$metadata'
        response = requests.get(url,auth=('admin', 'admin'))

        metadata = xmltodict.parse(response.text,force_list=('Property','PropertyRef'))
        metadata_schema = ((metadata.get('edmx:Edmx') or {}).get('edmx:DataServices') or {}).get('Schema') or {}
        metadata_entity_types = metadata_schema.get('EntityType') or []
        #metadata_complex_types = metadata_schema.get('ComplexType') or []
        

        for item in metadata_entity_types:

            item_name = item.get('@Name')

            if item_name.startswith(REGISTER_TYPES) and item_name.endswith("_RecordType"):
            # регистр с постфиксом RecordType содержит описание полей регистра и описание ключа
                item_name = item_name.removesuffix("_RecordType")
                properties = self._read_metadata_item_properties(item) 
                primary_key = self._read_metadata_item_key(item,properties)
                self[item_name] = MetadataObject1C(properties,primary_key)

            elif item_name.startswith(ENTITY_TYPES) and not item_name.endswith(METADATA_POSTFIXES):
            # если документ или справочник без постфикса, то
            # читаем его описание полей и ключ
            # (также может быть табличная часть документа или справочника)
                properties = self._read_metadata_item_properties(item)
                primary_key = self._read_metadata_item_key(item,properties)
                self[item_name] = MetadataObject1C(properties,primary_key)

