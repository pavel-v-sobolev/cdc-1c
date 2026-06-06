import requests
import logging

import xmltodict

from cdc_1C.DataReader1C import DataReader1C
from cdc_1C.MetadataReader1C import MetadataReader1C

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ChangeReader1C(DataReader1C):
    def __init__(self, base_url: str, exchange_name: str, queue_guid: str,
                 metadata: MetadataReader1C, name_mapper=None):
        super().__init__(base_url, metadata, name_mapper)
        self.exchange_name = exchange_name
        self.queue_guid = queue_guid

    def read_changes(self):
        message_no = self.get_last_received_no()+1

        url = f"{self.base_url}/SelectChanges?DataExchangePoint='{self.base_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={message_no}"

        response = requests.post(url,auth=('admin', 'admin'))

        change_data = xmltodict.parse(response.text,force_list=('d:element','entry'))
        change_entries = (change_data.get('feed') or {}).get('entry') or []

        self.read_data_entries(change_entries)



    def get_last_received_no(self)->int:
        """
        Получить номер последнего пакета обмена, который был получен и подтвержден
        """
        url = f"{self.base_url}/ExchangePlan_{self.exchange_name}?$format=json"
        response = requests.get(url,auth=('admin', 'admin'))
        queues_data = response.json()

        queues = queues_data.get('value') or []
        receive_no = 0

        for queue in queues:
            if self.queue_guid == queue['Ref_Key']:
                receive_no = int(queue['ReceivedNo'])
        
        return receive_no
    

