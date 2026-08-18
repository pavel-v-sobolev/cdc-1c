"""
Точка входа ТОЛЬКО для обработчиков — без репликатора вообще.

Такой процесс можно поднять в отдельном контейнере, перезапускать и обновлять независимо от
загрузки: с репликаторами его связывает одна лишь БД.

    репликатор            handlers_1c              обработчик
    ──────────            ───────────              ──────────
    сохранил объект  →  update_is_required=true  →  увидел флаг, посчитал витрину,
                                                    снял флаг и записал last_run_at

Подписку обработчик объявляет сам: при старте он пишет свои таблицы в handlers_1c.update_on,
а репликатор эту колонку читает. Поэтому репликатору не нужны ни импорт обработчика, ни его код.

Верхняя граница окна берётся из общей таблицы merges_in_process_1c: обработчик обязан видеть
незавершённые merge ЛЮБОГО репликатора, даже из чужого контейнера. Их строки уже имеют merged_on
в прошлом, но ещё не видны, и граница, взятая как «сейчас», их бы перешагнула — потеря строк молча.
"""

import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine

from cdc_1c import HandlerLoop

from handlers import ZakazyKlientov, ZakazyKlientovGrouped

DB_SCHEMA = os.environ.get("CDC1C_DB_SCHEMA")

# Соединения: по одному на поток обработчика плюс запас на служебные запросы.
engine = create_engine(os.environ["CDC1C_DB_URL"], pool_size=4)

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

# run_forever блокирующий — тот же, что у репликатора, поэтому запуск выглядит одинаково.
with ThreadPoolExecutor(max_workers=2, thread_name_prefix='cdc') as pool:
    zakazy_klientov_loop = pool.submit(handler_zakazy_klientov.run_forever)
    zakazy_klientov_grouped_loop = pool.submit(handler_zakazy_klientov_grouped.run_forever)

    zakazy_klientov_loop.result()
    zakazy_klientov_grouped_loop.result()
