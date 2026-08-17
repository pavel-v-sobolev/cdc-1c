"""
Точка входа для НЕСКОЛЬКИХ планов обмена, у которых общий обработчик.

Отличий от runner.py три:

1. На каждый план обмена — свой Replicator1C (свои exchange_name и queue_guid), но engine и схема
   у них общие: пишут они в одну целевую БД.
2. HandlerRunner'ы создаются отдельно и передаются ОБОИМ репликаторам — те же самые объекты. Так у
   обработчиков остаются общими и очередь грязных отметок (иначе обработчик не узнал бы про
   изменения чужого плана), и реестр незавершённых merge (иначе граница его окна не была бы прижата
   к merge соседа, и строки этого merge, у которых merged_on уже в прошлом, оказались бы левее
   отметки — потерялись бы молча). Реестр раздаёт первый репликатор, второй подхватывает его же.
3. run_forever блокирует поток, поэтому репликаторы запускаются в пуле. Они равнозначны: каждый
   заявляет себя пользователем раннера на входе и снимает заявку на выходе, а поток обработчиков
   гаснет, когда выйдет последний.

Останавливается всё по SIGTERM/SIGINT — перехват процессный, флаг получают все циклы разом, в каком
бы потоке они ни крутились.

Если планы обмена НЕ пересекаются по объектам и каждый обработчик читает таблицы только своего
плана — всё это не нужно: разносите планы по отдельным процессам с обычным runner.py.
"""

import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine

from cdc_1c import HandlerRunner, Replicator1C

from handlers import ZakazyKlientov, ZakazyKlientovGrouped

FULL_LOAD_WORKERS = 2
POLL_INTERVAL = 60

ODATA_URL = os.environ["CDC1C_ODATA_URL"]
ODATA_AUTH = (os.environ["CDC1C_ODATA_USER"], os.environ["CDC1C_ODATA_PASSWORD"])
DB_SCHEMA = os.environ.get("CDC1C_DB_SCHEMA")

# Соединения нужны всем сразу: цикл изменений каждого репликатора, страницы каждой полной выгрузки
# и по потоку на каждого обработчика. Здесь репликаторов два и обработчиков два, отсюда
# 2 * FULL_LOAD_WORKERS + 2 + 2.
engine = create_engine(os.environ["CDC1C_DB_URL"],
                       pool_size=2 * FULL_LOAD_WORKERS + 4)

# На каждого обработчика свой раннер со своим потоком. Оба уходят в оба репликатора — в этом
# весь смысл файла.
zakazy_klientov = HandlerRunner(
    engine=engine,
    schema=DB_SCHEMA,
    handler=ZakazyKlientov(),
)

zakazy_klientov_grouped = HandlerRunner(
    engine=engine,
    schema=DB_SCHEMA,
    handler=ZakazyKlientovGrouped(),
)

HANDLER_RUNNERS = [zakazy_klientov, zakazy_klientov_grouped]

replicator1 = Replicator1C(
    odata_url=ODATA_URL,
    odata_auth=ODATA_AUTH,
    exchange_name=os.environ["CDC1C_EXCHANGE_NAME"],
    queue_guid=os.environ["CDC1C_QUEUE_GUID"],
    engine=engine,
    db_schema=DB_SCHEMA,
    full_load_workers=FULL_LOAD_WORKERS,
    handler_runners=HANDLER_RUNNERS,
)

replicator2 = Replicator1C(
    odata_url=ODATA_URL,
    odata_auth=ODATA_AUTH,
    exchange_name=os.environ["CDC1C_EXCHANGE_NAME_2"],
    queue_guid=os.environ["CDC1C_QUEUE_GUID_2"],
    engine=engine,
    db_schema=DB_SCHEMA,
    full_load_workers=FULL_LOAD_WORKERS,
    handler_runners=HANDLER_RUNNERS,
)

with ThreadPoolExecutor(max_workers=2, thread_name_prefix='replicator') as pool:
    replicator1_loop = pool.submit(replicator1.run_forever, interval=POLL_INTERVAL)
    replicator2_loop = pool.submit(replicator2.run_forever, interval=POLL_INTERVAL)

    replicator1_loop.result()
    replicator2_loop.result()
