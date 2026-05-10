import requests
import logging
from typing import Any

import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


exchange_name = 'ДляODATA'
queue_guid = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'
base_url = "http://192.168.56.101/trade_demo/odata/standard.odata"

GUESS_UUID_TYPES = True
# Проблема в том, что 1С часть GUID полей присылает как строки в описании метаданных.
# В этом модуле есть логика, которая определяет тип UUID поля, по имени поля "Recorder" 
# или по наличию другого поля с постфиксом "_Type" для составных типов данных.
# На всякий случай сделан этот флаг, чтобы можно было эту логику отключить.
# Конечно, если отключить флаг, то это приведет к сущетвенному снижению быстродействия, т.к. часть полей будут
# UUID, а часть VARCHAR

type_mapping = {'Edm.Guid':Uuid(),
                'Edm.Int64':BigInteger(),
                'Edm.Int16':SmallInteger(),
                'Edm.String':String(),
                'Edm.Double':Numeric(),
                'Edm.Boolean':Boolean(), 
                'Edm.DateTime':DateTime()}

REGISTER_TYPES = ('InformationRegister','AccumulationRegister')
ENTITY_TYPES = ('Catalog','Document')
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'

def get_last_received_no()->int:
    
    url = f"{base_url}/ExchangePlan_{exchange_name}?$format=json"
    response = requests.get(url,auth=('admin', 'admin'))
    queues_data = response.json()

    queues = queues_data.get('value') or []
    receive_no = 0

    for queue in queues:
        if queue_guid == queue['Ref_Key']:
            receive_no = int(queue['ReceivedNo'])
    
    return receive_no

message_no = get_last_received_no()+1

def read_metadata_item_properties(item):

    item_properties = item.get('Property')

    properties = {}
    for item_property in item_properties:
        property_name = item_property['@Name']
        property_type = item_property['@Type']
        
        if GUESS_UUID_TYPES:
            if property_name=='Recorder':
                # Принудительно ставим Uuid для регистраторов, т.к. 1С почему-то присылает String
                property_type='Edm.Guid'            

        if property_type in type_mapping:
            properties[property_name] = type_mapping[property_type]
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
                    properties[uuid_property_name] = Uuid()


    return properties

def read_metadata_item_key(item):
    key = []
    item_key = (item.get('Key') or {}).get('PropertyRef')

    if item_key:
        key = [k.get('@Name') for k in item_key if k.get('@Name') is not None]

    return key

def read_metadata():
    url = f'{base_url}/$metadata'
    response = requests.get(url,auth=('admin', 'admin'))

    metadata = xmltodict.parse(response.text,force_list=('Property','PropertyRef'))
    metadata_schema = ((metadata.get('edmx:Edmx') or {}).get('edmx:DataServices') or {}).get('Schema') or {}
    metadata_entity_types = metadata_schema.get('EntityType') or []
    #metadata_complex_types = metadata_schema.get('ComplexType') or []


    metadata_properties = {}
    metadata_keys = {}

    

    for item in metadata_entity_types:

        item_name = item.get('@Name')

        if item_name.startswith(REGISTER_TYPES) and item_name.endswith("_RecordType"):
        # регистр с постфиксом RecordType содержит описание полей регистра и описание ключа
            item_name = item_name.removesuffix("_RecordType")
            metadata_properties[item_name] = read_metadata_item_properties(item) 
            metadata_keys[item_name] = read_metadata_item_key(item)

        elif item_name.startswith(ENTITY_TYPES) and not item_name.endswith(METADATA_POSTFIXES):
        # если документ или справочник без постфикса, то
        # читаем его описание полей и ключ
        # (также может быть табличная часть документа или справочника)
            metadata_properties[item_name] = read_metadata_item_properties(item)
            metadata_keys[item_name] = read_metadata_item_key(item)

    return metadata_properties, metadata_keys    



metadata_properties, metadata_keys = read_metadata()

def parce_object_full_name(object_full_name):
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
    return object_name,object_type


def get_record_fields(properties:dict):
    # обычные поля
    fields = {k.removeprefix('d:'):v for k,v in properties.items() 
                if k.startswith('d:') and not k.endswith('_Type') and not isinstance(v,dict)}
    
    # поля составных типов (убираем ODATA_PREFIX в значении)
    fields_type = {k.removeprefix('d:'):v.removeprefix(ODATA_PREFIX) for k,v in properties.items() 
                    if k.startswith('d:') and k.endswith('_Type') and not isinstance(v,dict)}
    
    fields = fields | fields_type
    
    return fields

def get_register_records(properties:dict):
    # Функция забирает заииси регистра, которые приходят по одному регистратору. Записей может быть несколько.
    # Она глобальную структуру changes, дописывая туда записи регистра.
    # Сhanges - это словарь, в котором ключом является объект 1С, например "Document_ЗаказКлиента",
    # значения содержат массив записей в виде

    recorder = properties.get('d:Recorder')
    recorder_type = properties.get('d:Recorder_Type')

    records = (properties.get('d:RecordSet') or {}).get('d:element') or []

    new_records = []
    for record in records:
        fields = get_record_fields(record)
        new_records.append(fields)

    if not new_records:
        # Если записей нет, то значит запись удалена, создаем пустую запись.
        if recorder and recorder_type:
            recorder_name, _ = parce_object_full_name(recorder_type)
            new_records = [{'Recorder':recorder, 'Recorder_Type':recorder_name}]
        else:
            logger.error(f'No recorder or recorder type for {object_name}')
           
    add_records_to_all_changes(object_name,new_records)


def get_record_table_parts(properties):
    # ищем табличные части в свойствах объекта
    # если ти данных dict и если префикс в названии 'd:', то будем считать что это табличная часть

    table_parts = {k.removeprefix('d:'):v for k,v in properties.items() 
                    if k.startswith('d:') and isinstance(v,dict)}
    return table_parts

def add_records_to_all_changes(object_name,new_records:list):
    if new_records:
        if object_name in changes.keys():
            changes[object_name].extend(new_records)
        else:
            changes[object_name] = new_records

def get_entity_records(properties:dict):
    """
    Функция забирает поля документа документа или справочника.
    Запись одна, но могут быть табличные части, которые будут записаны в отдельные элементы структуры changes
    """
    
    fields = get_record_fields(properties)

    add_records_to_all_changes(object_name,[fields])

    table_parts = get_record_table_parts(properties)
    
    for table_part_key,table_part in table_parts.items():
        table_part_full_name = table_part.get('@m:type')
        if table_part_full_name:
            table_part_name, _ = parce_object_full_name(table_part_full_name)
            table_part_rows = table_part.get('d:element') or []
            if table_part_rows:
                for table_part_row in table_part_rows:
                    table_part_fields = get_record_fields(table_part_row)
                    add_records_to_all_changes(table_part_name,[table_part_fields])

        else:
            logger.error(f'Can not determine table part for field {table_part_key}')



url = f"{base_url}/SelectChanges?DataExchangePoint='{base_url}/ExchangePlan_{exchange_name}(guid'{queue_guid}')'&MessageNo={message_no}"
response = requests.post(url,auth=('admin', 'admin'))
change_data = xmltodict.parse(response.text,force_list=('d:element','entry'))
change_entries = (change_data.get('feed') or {}).get('entry') or []

global changes
changes = {}

for change_entry in change_entries:

    object_id = change_entry.get('id')
    object_full_name = (change_entry.get('category') or {}).get('@term')
    object_name, object_type = parce_object_full_name(object_full_name)

    logger.info(f'Parcing {object_name}')
    
    properties = (change_entry.get('content') or {}).get('m:properties') or {}

    if object_type in REGISTER_TYPES:
        get_register_records(properties)

    if object_type in ENTITY_TYPES:
        get_entity_records(properties)


change_data['feed']['entry'][1]['content']['m:properties']['d:ЭтапыГрафикаОплаты']['d:element']

change_data['feed']['entry'][3]['content']

change_data['feed'].keys()
