import requests
import logging
from typing import Any

import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime

from cdc_1C import MetadataReader, DataReader

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


exchange_name = 'ДляODATA'
queue_guid = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'
base_url = "http://192.168.56.101/trade_demo/odata/standard.odata"


metadata = MetadataReader(base_url)



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


order_data = DataReader(base_url, metadata)

order_data.read_object('Document_ЗаказКлиента')


# url = f"{base_url}/SelectChanges?DataExchangePoint='{base_url}/ExchangePlan_{exchange_name}(guid'{queue_guid}')'&MessageNo={message_no}"

# response = requests.post(url,auth=('admin', 'admin'))

# change_data = xmltodict.parse(response.text,force_list=('d:element','entry'))
# change_entries = (change_data.get('feed') or {}).get('entry') or []



pass