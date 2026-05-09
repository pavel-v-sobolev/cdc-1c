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

type_mapping = {'Edm.Guid':Uuid(),
                'Edm.Int64':BigInteger(),
                'Edm.Int16':SmallInteger(),
                'Edm.String':String(),
                'Edm.Double':Numeric(),
                'Edm.Boolean':Boolean(), 
                'Edm.DateTime':DateTime()}

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


def read_metadata():
    url = f'{base_url}/$metadata'
    response = requests.get(url,auth=('admin', 'admin'))

    metadata = xmltodict.parse(response.text,force_list=('Property','PropertyRef'))
    metadata_schema = ((metadata.get('edmx:Edmx') or {}).get('edmx:DataServices') or {}).get('Schema') or {}
    metadata_entity_types = metadata_schema.get('EntityType') or []
    metadata_complex_types = metadata_schema.get('ComplexType') or []

    allowed_objects = ('InformationRegister_','AccumulationRegister_','Catalog_','Document_')

    metadata_properties = {}
    metadata_keys = {}
    for item in metadata_entity_types+metadata_complex_types:

        item_name = item.get('@Name')

        if not item_name.startswith(allowed_objects):
            continue

        item_name = item_name.removesuffix("_RowType")
        item_name = item_name.removesuffix("_RecordType")


        item_key = (item.get('Key') or {}).get('PropertyRef')

        if item_key:
            key = [k.get('@Name') for k in item_key if k.get('@Name') is not None]
            metadata_keys[item_name] = key

        item_properties = item.get('Property')

        properties = {}
        for item_property in item_properties:
            property_name = item_property['@Name']
            property_type = item_property['@Type']
            
            if property_name=='Recorder':
                # Принудительно ставим Uuid для регистраторов, т.к. 1С почему-то присылает String
                property_type='Edm.Guid'

            if property_type in type_mapping:
                properties[property_name] = type_mapping[property_type]
            else:
                if 'Collection' not in property_type:
                    logger.error(f'Property {property_name} has unknown type {property_type}')
            
            metadata_properties[item_name] = properties   
    return metadata_properties, metadata_keys    


ODATA_PREFIX = 'StandardODATA.'

metadata_properties, metadata_keys = read_metadata()

def parce_object_full_name(object_full_name):
    if object_full_name is None:
        logger.error(f'Object full name is None')
        return None, None
    
    object_name = object_full_name.removeprefix(ODATA_PREFIX)
    
    if '_' in object_name:
        object_type = object_name.split('_')[0]
    else:
        logger.error(f'Object type not found in object full name {object_full_name}')
        return None, None
    return object_name,object_type

def get_recorder_records(properties:dict)->tuple[str,list[dict[str,Any]]]:

    records = (properties.get('d:RecordSet') or {}).get('d:element') or []
    type_property = '@m:type'

    result = []

    for record in records:

        fields = {k.removeprefix('d:'):v for k,v in record.items()}

        del fields[type_property]

        if 'Recorder_Type' in fields.keys():
            fields['Recorder_Type'] = fields['Recorder_Type'].removeprefix(ODATA_PREFIX)

        result.append(fields)
    
    return result



url = f"{base_url}/SelectChanges?DataExchangePoint='{base_url}/ExchangePlan_{exchange_name}(guid'{queue_guid}')'&MessageNo={message_no}"
response = requests.post(url,auth=('admin', 'admin'))
change_data = xmltodict.parse(response.text,force_list=('d:element','entry'))
change_entries = (change_data.get('feed') or {}).get('entry') or []

changes = {}

for change_entry in change_entries:

    object_id = change_entry.get('id')
    object_full_name = (change_entry.get('category') or {}).get('@term')
    object_name, object_type = parce_object_full_name(object_full_name)

    logger.info(f'Parcing {object_name}')
    
    properties = (change_entry.get('content') or {}).get('m:properties') or {}

    if object_type in ['InformationRegister','AccumulationRegister']:
        records = get_recorder_records(properties)
        

    pass



change_data['feed']['entry'][1]['content']['m:properties']['d:ЭтапыГрафикаОплаты']['d:element']

change_data['feed']['entry'][3]['content']

change_data['feed'].keys()
