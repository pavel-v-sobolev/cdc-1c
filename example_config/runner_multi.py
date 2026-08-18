"""
Точка входа для НЕСКОЛЬКИХ планов обмена, у которых общий обработчик.

Отличий от runner.py три:

1. На каждый план обмена — свой Replicator1C (свои exchange_name и queue_guid), но engine и схема
   у них общие: пишут они в одну целевую БД.
2. Обработчики запускаются отдельно и о репликаторах не знают: сигналы им идут через handlers_1c,
   а незавершённые merge оба репликатора публикуют в merges_in_process_1c. Поэтому обработчику
   безразлично, сколько планов обмена его кормит и в одном ли они с ним процессе.
3. run_forever блокирует поток и у репликатора, и у обработчика, поэтому все четыре цикла уходят
   в один пул. Они равнозначны и друг о друге не знают.

Останавливается всё по SIGTERM/SIGINT — перехват процессный, флаг получают все циклы разом, в каком
бы потоке они ни крутились.

Если планы обмена НЕ пересекаются по объектам и каждый обработчик читает таблицы только своего
плана — всё это не нужно: разносите планы по отдельным процессам с обычным runner.py.
"""

import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine

from cdc_1c import HandlerLoop, Replicator1C

from handlers import ZakazyKlientov, ZakazyKlientovGrouped

FULL_LOAD_WORKERS = 2
POLL_INTERVAL = 60

ODATA_URL = os.environ["CDC1C_ODATA_URL"]
ODATA_AUTH = (os.environ["CDC1C_ODATA_USER"], os.environ["CDC1C_ODATA_PASSWORD"])
DB_SCHEMA = os.environ.get("CDC1C_DB_SCHEMA")

# Соединения нужны всем сразу: цикл изменений и поток отметки живости у каждого репликатора,
# страницы каждой полной выгрузки и по потоку на каждого обработчика. Здесь репликаторов два и
# обработчиков два, отсюда 2 * FULL_LOAD_WORKERS + 2 * 2 + 2.
engine = create_engine(os.environ["CDC1C_DB_URL"],
                       pool_size=2 * FULL_LOAD_WORKERS + 6)

# На каждого обработчика свой цикл в своём потоке. Репликаторам они не передаются — те узнают
# о подписках из handlers_1c.
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

replicator1 = Replicator1C(
    odata_url=ODATA_URL,
    odata_auth=ODATA_AUTH,
    exchange_name=os.environ["CDC1C_EXCHANGE_NAME"],
    queue_guid=os.environ["CDC1C_QUEUE_GUID"],
    engine=engine,
    db_schema=DB_SCHEMA,
    full_load_workers=FULL_LOAD_WORKERS,
)

replicator2 = Replicator1C(
    odata_url=ODATA_URL,
    odata_auth=ODATA_AUTH,
    exchange_name=os.environ["CDC1C_EXCHANGE_NAME_2"],
    queue_guid=os.environ["CDC1C_QUEUE_GUID_2"],
    engine=engine,
    db_schema=DB_SCHEMA,
    full_load_workers=FULL_LOAD_WORKERS,
)

with ThreadPoolExecutor(max_workers=4, thread_name_prefix='cdc') as pool:
    replicator1_loop = pool.submit(replicator1.run_forever, interval=POLL_INTERVAL)
    replicator2_loop = pool.submit(replicator2.run_forever, interval=POLL_INTERVAL)
    zakazy_klientov_loop = pool.submit(handler_zakazy_klientov.run_forever)
    zakazy_klientov_grouped_loop = pool.submit(handler_zakazy_klientov_grouped.run_forever)

    # Результат забираем обязательно: исключение, с которым цикл упал, иначе осталось бы лежать
    # внутри задачи непрочитанным, и процесс молча продолжил бы работать остальными.
    replicator1_loop.result()
    replicator2_loop.result()
    zakazy_klientov_loop.result()
    zakazy_klientov_grouped_loop.result()
