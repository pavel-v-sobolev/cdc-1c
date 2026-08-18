"""
Ручной отладочный вход: прогон компонентов против живой 1С и dev-Postgres из-под отладчика.
Не тест — pytest его не собирает (имя не начинается с `test_`), и в пакет он не попадает.

Запускается целиком (`python tests/debug.py`) или построчно из-под отладчика: до `main()` идут
только присваивания, поэтому любой блок можно выполнить отдельно, оставив остальные закомментированными.

Параметры подключения дублируются в live-тестах (test_cdc_run_once.py, test_run_forever_live.py) —
меняя их здесь, поменяйте и там.
"""

import logging

from sqlalchemy import create_engine

from cdc_1c import ChangeReader1C, DBWriter1C, MetadataReader1C, NameMapper1C, Replicator1C

ODATA_URL = "http://192.168.56.102/trade_demo/odata/standard.odata"
ODATA_AUTH = ('admin', 'admin')
EXCHANGE_NAME = 'ДляODATA'
QUEUE_GUID = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'
DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c"
DB_SCHEMA = 'cdc_1c_trade_demo'


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    engine = create_engine(DB_URL)

    # Компоненты по отдельности — чтобы смотреть промежуточный результат каждого.
    metadata = MetadataReader1C(ODATA_URL, odata_auth=ODATA_AUTH, engine=engine, schema=DB_SCHEMA)
    changes = ChangeReader1C(ODATA_URL, EXCHANGE_NAME, QUEUE_GUID, metadata, odata_auth=ODATA_AUTH)
    writer = DBWriter1C(engine=engine, name_mapper=NameMapper1C(), schema=DB_SCHEMA)

    changes.read_changes()
    for object_name, data_object in changes.items():
        result = writer.save(object_name, data_object)
        print(object_name, result)
    # Подтверждение приёма намеренно НЕ отправляется: без него пакет остаётся в очереди 1С и
    # отладку можно повторять сколько угодно раз. Нужно списать — раскомментируйте.
    # changes.notify_changes_received()

    # То же самое целиком, оркестратором.
    replicator = Replicator1C(odata_url=ODATA_URL, odata_auth=ODATA_AUTH,
                              exchange_name=EXCHANGE_NAME, queue_guid=QUEUE_GUID,
                              engine=engine, db_schema=DB_SCHEMA)
    print(replicator.list_objects())
    replicator.run_once(notify_changes=False)


if __name__ == "__main__":
    main()
