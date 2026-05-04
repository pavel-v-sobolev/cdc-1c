import requests
import logging
import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime


logging.basicConfig()
logger = logging.getLogger('dbmerge')
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

def get_last_received_no():
    
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

    metadata = xmltodict.parse(response.text)
    metadata_schema = ((metadata.get('edmx:Edmx') or {}).get('edmx:DataServices') or {}).get('Schema') or {}
    metadata_entities = metadata_schema.get('EntityType') or []

    entities = {}
    for metadata_entity in metadata_entities:
        entity_name = metadata_entity.get('@Name')
        entity_properties = metadata_entity.get('Property')

        properties = {}
        for entity_property in entity_properties:
            property_name = entity_property['@Name']
            property_type = entity_property['@Type']
            
            if property_type in type_mapping:
                properties[property_name] = type_mapping[property_type]
            else:
                if 'Collection' not in property_type:
                    logger.error(f'Property {property_name} has unknown type {property_type}')
            
            entities[entity_name] = properties   
    return entities    


prefix = 'StandardODATA.'



url = f"{base_url}/SelectChanges?DataExchangePoint='{base_url}/ExchangePlan_{exchange_name}(guid'{queue_guid}')'&MessageNo={message_no}"

response = requests.post(url,auth=('admin', 'admin'))


change_data = xmltodict.parse(response.text)
change_entries = (change_data.get('feed') or {}).get('entry') or []

for change_entry in change_entries:
    object_type = change_entry['category']['@term']
    object_type = object_type.replace(prefix,'')
    print(object_type)

change_data['feed']['entry'][0]['content']['m:properties']

change_data['feed']['entry'][3]['content']

change_data['feed'].keys()
