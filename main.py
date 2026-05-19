import requests
import logging
from typing import Any

import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime

from cdc_1C import MetadataReader, DataReader, ChangeReader

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


exchange_name = 'ДляODATA'
queue_guid = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'
base_url = "http://192.168.56.101/trade_demo/odata/standard.odata"


metadata = MetadataReader(base_url)

# order_data = DataReader(base_url, metadata)
# order_data.read_object('Document_ЗаказКлиента')

changes = ChangeReader(base_url, exchange_name, queue_guid, metadata)
changes.read_changes()


pass