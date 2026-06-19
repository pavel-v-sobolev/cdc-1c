import requests
import logging

import xmltodict

from cdc_1c.data_reader import DataReader1C
from cdc_1c.metadata_reader import MetadataReader1C

logger = logging.getLogger(__name__)


class ChangeReader1C(DataReader1C):
    def __init__(self, odata_url: str, exchange_name: str, queue_guid: str,
                 metadata: MetadataReader1C, odata_auth: tuple[str, str] | None = None,
                 request_timeout: float | None = None):
        super().__init__(odata_url, metadata, odata_auth, request_timeout)
        self.exchange_name = exchange_name
        self.queue_guid = queue_guid
        self.message_no = 0

    def read_changes(self):
        # Сбрасываем накопленные данные предыдущего цикла (важно для run_forever).
        self.clear()
        self.message_no = self.get_last_received_no()+1
        self.exchange_message_no = self.message_no

        logger.info(f"Reading changes from 1C (message {self.message_no})")

        url = f"{self.odata_url}/SelectChanges?DataExchangePoint='{self.odata_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={self.message_no}"

        response = requests.post(url,auth=self.odata_auth,timeout=self.request_timeout)
        response.raise_for_status()

        change_data = xmltodict.parse(response.text,force_list=('d:element','entry'))
        change_entries = (change_data.get('feed') or {}).get('entry') or []

        self.read_data_entries(change_entries)

    def notify_changes_received(self):
        """
        Подтвердить получение изменений, отправив запрос на сервер
        """
        url = f"{self.odata_url}/NotifyChangesReceived?DataExchangePoint='{self.odata_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={self.message_no}"
        response = requests.post(url,auth=self.odata_auth,timeout=self.request_timeout)
        # Не-2xx -> HTTPError. Подтверждение не прошло — изменения не списаны и придут снова
        # (в run_forever цикл повторится, save идемпотентен).
        response.raise_for_status()
        logger.info(f"Changes confirmed for queue {self.queue_guid} (message {self.message_no})")


    def get_last_received_no(self)->int:
        """
        Получить номер последнего пакета обмена, который был получен и подтвержден
        """
        url = f"{self.odata_url}/ExchangePlan_{self.exchange_name}?$format=json"
        response = requests.get(url,auth=self.odata_auth,timeout=self.request_timeout)
        response.raise_for_status()
        queues_data = response.json()

        queues = queues_data.get('value') or []
        receive_no = 0

        for queue in queues:
            if self.queue_guid == queue['Ref_Key']:
                receive_no = int(queue['ReceivedNo'])
        
        return receive_no
    

