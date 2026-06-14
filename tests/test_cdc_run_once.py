"""
Живой smoke-тест Replicator1C против реального сервера 1С (тестовая база торговли) и
dev-Postgres. Параметры подключения — те же, что в main.py.

Гоняет полный цикл Replicator1C.run_once(notify_changes=False): read → save БЕЗ notify, чтобы
не списывать изменения из очереди обмена 1С и оставить прогон повторяемым. Проверяет, что
метаданные читаются с живой 1С и изменения сохраняются в БД без ошибок.

Требует доступной 1С и Postgres (помечен маркером integration — отдельной обработки оффлайна нет,
без доступа тест упадёт). Запуск:
  - через pytest: `uv run pytest tests/test_cdc_run_once.py`;
  - напрямую, без pytest: `uv run python tests/test_cdc_run_once.py`.
"""

import pytest
from sqlalchemy import create_engine, inspect

from cdc_1c import Replicator1C

# Параметры из main.py — тестовый/dev-контур, не боевой.
ODATA_URL = "http://192.168.56.101/trade_demo/odata/standard.odata"
ODATA_USER = "admin"
ODATA_PASSWORD = "admin"
EXCHANGE_NAME = "ДляODATA"
QUEUE_GUID = "a9bc23c5-3689-11f1-926c-0800270bc6cb"
DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c"
DB_SCHEMA = "cdc_1c_trade_demo"



@pytest.mark.integration
def test_run_once_against_live_1c():

    repl = Replicator1C(
        odata_url=ODATA_URL,
        odata_user=ODATA_USER,
        odata_password=ODATA_PASSWORD,
        exchange_name=EXCHANGE_NAME,
        queue_guid=QUEUE_GUID,
        engine=create_engine(DB_URL),
        db_schema=DB_SCHEMA,
    )

    # Полный цикл read → save без notify: изменения не списываются из очереди, прогон повторяемый.
    repl.run_once(notify_changes=False)

    # Метаданные реально прочитаны; если были изменения — в целевой схеме есть таблицы.
    assert len(repl.metadata) > 0
    if len(repl.changes) > 0:
        assert inspect(repl.engine).get_table_names(schema=DB_SCHEMA)


if __name__ == "__main__":
    # Обычный запуск без pytest: `uv run python tests/test_cdc_run_once.py`.
    import logging

    logging.basicConfig(level=logging.INFO)
    test_run_once_against_live_1c()
    print("OK: цикл read → save отработал (notify отключён, изменения остались в очереди).")
