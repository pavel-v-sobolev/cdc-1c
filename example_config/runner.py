"""
Точка входа: здесь собирается всё — подключения, параметры цикла и список обработчиков.

Параметры присваиваются явно, по одному, а не пачкой из объекта настроек: на этом экране видно,
что именно передано в репликатор. Значения взяты из окружения (удобно для контейнера), но любое
из них можно заменить литералом — для python-приложения так и делается:

    odata_url="http://server/base/odata/standard.odata",
    odata_auth=("odata", "secret"),

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

# Например: разовая догрузка документов за период перед запуском цикла.
# replicator.full_load('Document_ЗаказКлиента', date_field='Date',
#                      date_from=date(2026, 1, 1), date_to=date(2026, 6, 30))

replicator.run_forever(
    interval=POLL_INTERVAL,
    handlers=[
        ZakazyKlientov(),
        ZakazyKlientovGrouped(),
        # Один и тот же обработчик можно завести несколько раз — но имена должны различаться:
        # имя это ключ состояния в handlers_1c, и одноимённые поделили бы одну отметку
        # last_run_at на двоих (раннер такой список отвергнет на старте).
        # ZakazyKlientov(name='ZakazyKlientov_mart2'),
    ],
)
