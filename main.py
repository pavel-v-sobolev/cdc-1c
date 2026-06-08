import requests
import logging
from typing import Any

import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime

from cdc_1C import MetadataReader1C, DataReader1C, ChangeReader1C, NameMapper1C

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


exchange_name = 'ДляODATA'
queue_guid = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'
base_url = "http://192.168.56.101/trade_demo/odata/standard.odata"


metadata = MetadataReader1C(base_url)

# order_data = DataReader(base_url, metadata)
# order_data.read_object('Document_ЗаказКлиента')
mapper = NameMapper1C()
changes = ChangeReader1C(base_url, exchange_name, queue_guid, metadata, mapper)
changes.read_changes()



changes['Document_ЗаказКлиента']
changes['AccumulationRegister_ЗаказыКлиентов']

changes.notify_changes_received()

pass