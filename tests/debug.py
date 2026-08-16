"""
Ручной отладочный вход: прогон компонентов против живой 1С и dev-Postgres из-под отладчика.
Не тест — pytest его не собирает (имя не начинается с `test_`), и в пакет он не попадает.
Параметры подключения отсюда используют live-тесты (test_cdc_run_once.py, test_run_forever_live.py).
"""

import requests
import logging
from typing import Any

import xmltodict
from sqlalchemy import String, Uuid, BigInteger, SmallInteger, Numeric, Boolean, DateTime, create_engine

from cdc_1c import MetadataReader1C, DataReader1C, ChangeReader1C, NameMapper1C, DBWriter1C, Replicator1C


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c")

exchange_name = 'ДляODATA'
queue_guid = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'
odata_url = "http://192.168.56.102/trade_demo/odata/standard.odata"


odata_auth=('admin', 'admin')
metadata = MetadataReader1C(odata_url, odata_auth=odata_auth)

# order_data = DataReader(odata_url, metadata)
# order_data.read_object('Document_ЗаказКлиента')
mapper = NameMapper1C()
changes = ChangeReader1C(odata_url, exchange_name, queue_guid, metadata, odata_auth=odata_auth)

writer = DBWriter1C(engine=engine, name_mapper=mapper, data_reader=changes, schema='cdc_1c_trade_demo')

replicator = Replicator1C(engine=engine, odata_url=odata_url, exchange_name=exchange_name,
                          queue_guid=queue_guid, odata_auth=odata_auth)

replicator.list_objects()

changes.read_changes()

writer.save_all()
changes['Document_ЗаказКлиента'].to_nested_records()
#changes.notify_changes_received()




pass