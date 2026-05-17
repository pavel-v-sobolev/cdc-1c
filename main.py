import requests
import logging
from typing import Any

import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime

from cdc_1C import MetadataReader

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


exchange_name = 'ДляODATA'
queue_guid = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'
base_url = "http://192.168.56.101/trade_demo/odata/standard.odata"


metadata = MetadataReader(base_url)



REGISTER_TYPES = ('InformationRegister','AccumulationRegister')
ENTITY_TYPES = ('Catalog','Document')
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'

def get_last_received_no()->int:
    """
    Получить номер последнего пакета обмена, который был получен и подтвержден
    """
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




def parce_object_full_name(object_full_name):
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
    return object_name,object_type


def get_record_fields(properties:dict):
    # обычные поля
    fields = {k.removeprefix('d:'):v for k,v in properties.items() 
                if k.startswith('d:') and not k.endswith('_Type') and not isinstance(v,dict)}
    
    # поля составных типов (убираем ODATA_PREFIX в значении)
    fields_type = {k.removeprefix('d:'):(v.removeprefix(ODATA_PREFIX) if isinstance(v, str) else v) for k,v in properties.items() 
                    if k.startswith('d:') and k.endswith('_Type') and not isinstance(v,dict)}
    
    fields = fields | fields_type
    
    return fields

def get_register_records(properties:dict):
    """
    Функция забирает заииси регистра, которые приходят по одному регистратору. Записей может быть несколько.
    Она глобальную структуру changes, дописывая туда записи регистра.
    Сhanges - это словарь, в котором ключом является объект 1С, например "Document_ЗаказКлиента",
    значения содержат массив записей в виде.    
    """

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
    """
    Ищем табличные части в свойствах объекта.
    Если тип данных dict и если префикс в названии 'd:', то будем считать что это табличная часть
    """

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




url = f"{base_url}/SelectChanges?DataExchangePoint='{base_url}/ExchangePlan_{exchange_name}(guid'{queue_guid}')'&MessageNo={message_no}"
url = f"{base_url}/Catalog_Номенклатура"

#response = requests.post(url,auth=('admin', 'admin'))

response = requests.get(url,auth=('admin', 'admin'))
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


pass