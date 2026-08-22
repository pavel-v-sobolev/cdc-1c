"""
Та же точка входа, что и runner.py, плюс полные выгрузки по расписанию (FullLoadCron).

Запускается так же: `python runner_cron.py`.

Зачем это нужно. Цикл изменений догоняет данные событиями, а полная выгрузка — сверяет: она читает
объект из 1С и возвращает число РЕАЛЬНО изменённых строк, то есть при исправном CDC отвечает нулём.
Ночная перегрузка свежего хвоста (последние сутки-двое) стоит недорого, а расхождение показывает
сразу — и тут же его выравнивает.

На объект приходится две строки: что грузим и по какому расписанию. Дальше объект уходит своим
потоком в тот же пул, что репликатор и обработчики, и останавливается тем же SIGTERM.

Имена — те, что видны в БД (латиница), как и у обработчиков: настраивая выгрузку, смотрят в базу и
в реестр metadata_objects_1c (колонки object_full_name_en / fields_en). Оригинальные имена 1С
("Document_ЗаказКлиента") тоже принимаются.

Отсюда получается и раскладка «только выгрузки, без чтения изменений» — вычёркиванием, как и
остальные: собрать Replicator1C (он исполнитель full_load: метаданные, пагинация, запись, журнал)
и просто не отправлять его run_forever в пул. Метаданные при этом читаются лениво первым же
прогоном, недоступная 1С не роняет процесс (следующее срабатывание попробует снова), добавленное
поле подхватывается следующим прогоном. Единственное, чего в таком процессе не будет: новые объекты
не встанут сами в очередь на первичную выгрузку — это делает цикл изменений; состав расписаний
задаёте вы.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from sqlalchemy import create_engine

from cdc_1c import FullLoadCron, HandlerLoop, Replicator1C

from handlers import ZakazyKlientov, ZakazyKlientovGrouped

FULL_LOAD_WORKERS = 2
POLL_INTERVAL = 60

# Пул соединений: цикл изменений, страницы полной выгрузки, поток отметки живости, каждый
# обработчик и КАЖДОЕ расписание — своим потоком. Здесь два обработчика и два расписания,
# отсюда FULL_LOAD_WORKERS + 2 + 2 + 2.
engine = create_engine(os.environ["CDC1C_DB_URL"], pool_size=FULL_LOAD_WORKERS + 6)

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
    # Не знаете guid узла — оставьте пустым: в лог выведется список узлов плана обмена.
    queue_guid=os.environ.get("CDC1C_QUEUE_GUID", ""),
    engine=engine,
    db_schema=DB_SCHEMA,
    full_load_workers=FULL_LOAD_WORKERS,
)

# Каждую ночь в 03:00 — хвост за трое суток по документу, где важна свежесть. Граница timedelta
# считается в момент срабатывания, поэтому окно едет вместе с процессом. Фильтр по периоду заодно
# делает выгрузку устойчивее: документы листаются через $skip, и выборка за пару прошедших суток,
# в отличие от всего объекта, почти не меняется под ногами.
zakazy_cron = FullLoadCron(replicator, "Document_ZakazKlienta", cron="0 3 * * *",
                           date_field="Date", date_from=timedelta(days=3))

# По воскресеньям в 02:00 — справочник целиком: он маленький, границы не нужны вовсе.
nomenklatura_cron = FullLoadCron(replicator, "Catalog_Nomenklatura", cron="0 2 * * 0")

with ThreadPoolExecutor(max_workers=5, thread_name_prefix='cdc') as pool:
    replicator_task = pool.submit(replicator.run_forever, interval=POLL_INTERVAL)
    zakazy_klientov_task = pool.submit(handler_zakazy_klientov.run_forever)
    zakazy_klientov_grouped_task = pool.submit(handler_zakazy_klientov_grouped.run_forever)
    zakazy_cron_task = pool.submit(zakazy_cron.run_forever)
    nomenklatura_cron_task = pool.submit(nomenklatura_cron.run_forever)

    replicator_task.result()
    zakazy_klientov_task.result()
    zakazy_klientov_grouped_task.result()
    zakazy_cron_task.result()
    nomenklatura_cron_task.result()
