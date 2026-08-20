"""
Точка входа: здесь собирается всё — подключения, параметры цикла и сами обработчики.

Запускается как обычный скрипт: `python runner.py`. Пакет handlers/ лежит рядом — Python кладёт
каталог скрипта в sys.path, поэтому импорт ниже его находит.

Параметры присваиваются явно, по одному, чтобы на этом экране было видно, что именно передано в
репликатор. Значения взяты из окружения (удобно для контейнера), но любое можно заменить литералом —
для python-приложения так и делается:

    odata_url="http://server/base/odata/standard.odata",
    odata_auth=("odata", "secret"),   # None, если 1С опубликована без авторизации

На каждого обработчика — свой HandlerLoop со своим циклом, поэтому тяжёлая витрина не задерживает
остальные. Порядок между ними при этом не определён — на «витрина поверх витрины считается после
базовой» полагаться нельзя.

Репликатор обработчиков не запускает и о них не знает: сигналы идут через таблицу handlers_1c.
Здесь они просто живут в одном процессе с ним — так проще. Отсюда и обе другие раскладки, каждая
правкой этого файла: разнести по контейнерам — убрать репликатор, останутся одни циклы обработчиков;
несколько планов обмена — завести второй Replicator1C и отправить его в тот же пул. В самих
обработчиках при этом не меняется ничего.

Запускаются все одинаково: у репликатора и у обработчика блокирующий run_forever, и оба уходят
в пул потоков. Останавливает всех SIGTERM.
"""

import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine

from cdc_1c import HandlerLoop, Replicator1C

from handlers import ZakazyKlientov, ZakazyKlientovGrouped

FULL_LOAD_WORKERS = 2
POLL_INTERVAL = 60

# Пул соединений: цикл изменений, страницы полной выгрузки, поток отметки живости и каждый
# обработчик своим потоком. Здесь обработчиков два, отсюда FULL_LOAD_WORKERS + 2 + 2.
engine = create_engine(os.environ["CDC1C_DB_URL"], pool_size=FULL_LOAD_WORKERS + 4)

DB_SCHEMA = os.environ.get("CDC1C_DB_SCHEMA")

handler_zakazy_klientov = HandlerLoop(
    engine=engine,
    schema=DB_SCHEMA,
    handler=ZakazyKlientov(),
)

handler_zakazy_klientov_grouped = HandlerLoop(
    engine=engine,
    schema=DB_SCHEMA,
    handler=ZakazyKlientovGrouped(),
)

replicator = Replicator1C(
    odata_url=os.environ["CDC1C_ODATA_URL"],
    odata_auth=(os.environ["CDC1C_ODATA_USER"], os.environ["CDC1C_ODATA_PASSWORD"]),
    exchange_name=os.environ["CDC1C_EXCHANGE_NAME"],
    queue_guid=os.environ["CDC1C_QUEUE_GUID"],
    engine=engine,
    db_schema=DB_SCHEMA,
    full_load_workers=FULL_LOAD_WORKERS,
)

with ThreadPoolExecutor(max_workers=3, thread_name_prefix='cdc') as pool:
    replicator_task = pool.submit(replicator.run_forever, interval=POLL_INTERVAL)
    zakazy_klientov_task = pool.submit(handler_zakazy_klientov.run_forever)
    zakazy_klientov_grouped_task = pool.submit(handler_zakazy_klientov_grouped.run_forever)

    replicator_task.result()
    zakazy_klientov_task.result()
    zakazy_klientov_grouped_task.result()
