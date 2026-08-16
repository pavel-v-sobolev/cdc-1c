"""
Точка входа: здесь собирается всё — подключения, параметры цикла и список обработчиков.

Запускается как обычный скрипт: `python runner.py`. Пакет handlers/ лежит рядом — Python кладёт
каталог скрипта в sys.path, поэтому импорт ниже его находит.

Параметры присваиваются явно, по одному, чтобы на этом экране было видно, что именно передано в
репликатор. Значения взяты из окружения (удобно для контейнера), но любое можно заменить литералом —
для python-приложения так и делается:

    odata_url="http://server/base/odata/standard.odata",
    odata_auth=("odata", "secret"),   # None, если 1С опубликована без авторизации

Порядок списка handlers = порядок вызова в пределах одного прохода: витрина, построенная поверх
другой витрины, должна идти после базовой.
"""

import os

from sqlalchemy import create_engine

from cdc_1c import Replicator1C

from handlers import ZakazyKlientov, ZakazyKlientovGrouped

FULL_LOAD_WORKERS = 2
POLL_INTERVAL = 60

# Пул соединений: одновременно их держат цикл изменений, страницы полной выгрузки и поток
# обработчиков, поэтому full_load_workers + 3 (см. README).
engine = create_engine(os.environ["CDC1C_DB_URL"], pool_size=FULL_LOAD_WORKERS + 3)

replicator = Replicator1C(
    odata_url=os.environ["CDC1C_ODATA_URL"],
    odata_auth=(os.environ["CDC1C_ODATA_USER"], os.environ["CDC1C_ODATA_PASSWORD"]),
    exchange_name=os.environ["CDC1C_EXCHANGE_NAME"],
    queue_guid=os.environ["CDC1C_QUEUE_GUID"],
    engine=engine,
    db_schema=os.environ.get("CDC1C_DB_SCHEMA"),
    full_load_workers=FULL_LOAD_WORKERS,
)

replicator.run_forever(
    interval=POLL_INTERVAL,
    handlers=[
        ZakazyKlientov(),
        ZakazyKlientovGrouped(),
    ],
)
