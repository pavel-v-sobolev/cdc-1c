"""
Ручной прогон Replicator1C.run_forever против живой 1С и dev-Postgres (параметры как в
main.py / test_cdc_run_once.py). Интервал опроса — 5 секунд, notify включён по умолчанию
(изменения подтверждаются после успешного сохранения).

Это НЕ pytest-тест: цикл бесконечный, поэтому файл без префикса test_ и pytest его не собирает.
Запуск вручную:

    uv run python tests/run_forever_live.py

Кидай изменения из 1С — каждые 5 секунд они вычитываются, сохраняются в Postgres и
подтверждаются. Остановка — Ctrl+C (graceful: дорабатывает текущий цикл и выходит).
"""

import logging

from sqlalchemy import create_engine

from cdc_1c import Replicator1C

# Параметры из main.py — тестовый/dev-контур, не боевой.
ODATA_URL = "http://192.168.56.101/trade_demo/odata/standard.odata"
ODATA_USER = "admin"
ODATA_PASSWORD = "admin"
EXCHANGE_NAME = "ДляODATA"
QUEUE_GUID = "a9bc23c5-3689-11f1-926c-0800270bc6cb"
DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c"
DB_SCHEMA = "cdc_1c_trade_demo"
POLL_INTERVAL = 5.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    repl = Replicator1C(
        odata_url=ODATA_URL,
        odata_auth=(ODATA_USER, ODATA_PASSWORD),
        exchange_name=EXCHANGE_NAME,
        queue_guid=QUEUE_GUID,
        engine=create_engine(DB_URL),
        db_schema=DB_SCHEMA,
    )
    # notify включён (run_once(notify_changes=True) по умолчанию) — изменения подтверждаются.
    repl.run_forever(interval=POLL_INTERVAL)
