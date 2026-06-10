import requests
import logging
from typing import Any

import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime, create_engine

from cdc_1c import MetadataReader1C, DataReader1C, ChangeReader1C, NameMapper1C, DBWriter1C

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c")

exchange_name = 'ДляODATA'
queue_guid = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'
base_url = "http://192.168.56.101/trade_demo/odata/standard.odata"


metadata = MetadataReader1C(base_url)

# order_data = DataReader(base_url, metadata)
# order_data.read_object('Document_ЗаказКлиента')
mapper = NameMapper1C()
changes = ChangeReader1C(base_url, exchange_name, queue_guid, metadata)

writer = DBWriter1C(engine=engine, name_mapper=mapper, data_reader=changes, schema='cdc_1c_trade_demo')



changes.read_changes()

writer.save_all()

#changes.notify_changes_received()

pass