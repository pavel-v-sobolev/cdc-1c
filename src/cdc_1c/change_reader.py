import requests
import logging

import xmltodict

from cdc_1c.data_reader import DataReader1C
from cdc_1c.metadata_reader import MetadataReader1C

logger = logging.getLogger(__name__)


class ChangeReader1C(DataReader1C):
    def __init__(self, base_url: str, exchange_name: str, queue_guid: str,
                 metadata: MetadataReader1C, auth: tuple[str, str] | None = None):
        super().__init__(base_url, metadata, auth)
        self.exchange_name = exchange_name
        self.queue_guid = queue_guid
        self.message_no = 0

    def read_changes(self):
        # Сбрасываем накопленные данные предыдущего цикла (важно для run_forever).
        self.clear()
        self.message_no = self.get_last_received_no()+1
        self.exchange_message_no = self.message_no

        url = f"{self.base_url}/SelectChanges?DataExchangePoint='{self.base_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={self.message_no}"

        response = requests.post(url,auth=self.auth)

        change_data = xmltodict.parse(response.text,force_list=('d:element','entry'))
        change_entries = (change_data.get('feed') or {}).get('entry') or []

        self.read_data_entries(change_entries)

    def notify_changes_received(self):
        """
        Подтвердить получение изменений, отправив запрос на сервер
        """
        url = f"{self.base_url}/NotifyChangesReceived?DataExchangePoint='{self.base_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={self.message_no}"
        response = requests.post(url,auth=self.auth)
        if response.status_code == 200:
            logger.info(f"Changes confirmed for queue {self.queue_guid}")
        else:
            logger.error(f"Failed to confirm changes for queue {self.queue_guid}. Status code: {response.status_code}")


    def get_last_received_no(self)->int:
        """
        Получить номер последнего пакета обмена, который был получен и подтвержден
        """
        url = f"{self.base_url}/ExchangePlan_{self.exchange_name}?$format=json"
        response = requests.get(url,auth=self.auth)
        queues_data = response.json()

        queues = queues_data.get('value') or []
        receive_no = 0

        for queue in queues:
            if self.queue_guid == queue['Ref_Key']:
                receive_no = int(queue['ReceivedNo'])
        
        return receive_no
    

