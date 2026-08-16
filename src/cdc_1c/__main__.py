"""
Entrypoint для запуска из окружения без единой строки своего кода: `python -m cdc_1c` или команда
`cdc-1c`. Все настройки — переменные окружения CDC1C_*; режим задаёт CDC1C_MODE: `loop`
(по умолчанию) запускает run_forever с периодом CDC1C_POLL_INTERVAL, `once` — один run_once.

Обязательные: CDC1C_ODATA_URL, CDC1C_EXCHANGE_NAME, CDC1C_QUEUE_GUID, CDC1C_DB_URL.
Необязательные: CDC1C_ODATA_USER, CDC1C_ODATA_PASSWORD (без пользователя — без авторизации),
CDC1C_DB_SCHEMA, CDC1C_FULL_LOAD_WORKERS, CDC1C_POLL_INTERVAL, CDC1C_LOG_LEVEL, CDC1C_MODE.

Обработчиков здесь нет: они объявляются кодом, а тут кода пользователя нет. Нужны обработчики —
берите за основу example_config/runner.py: там ровно та же сборка, только список обработчиков
передаётся в run_forever, а значения при желании заменяются литералами.
"""
import logging
import os

from sqlalchemy import create_engine

from cdc_1c.replicator import Replicator1C


def main() -> None:
    odata_user = os.environ.get("CDC1C_ODATA_USER")
    full_load_workers = int(os.environ.get("CDC1C_FULL_LOAD_WORKERS", "2"))

    # Пул: одновременно соединение держат цикл изменений, страницы полной выгрузки и поток
    # обработчиков, отсюда full_load_workers + 3 (см. README).
    engine = create_engine(os.environ["CDC1C_DB_URL"], pool_size=full_load_workers + 3)

    # Параметры присваиваются явно, по одному — как и в example_config/runner.py: сборка одинаково
    # читается и здесь, и в пользовательском коде, где значения будут литералами.
    replicator = Replicator1C(
        odata_url=os.environ["CDC1C_ODATA_URL"],
        odata_auth=(odata_user, os.environ.get("CDC1C_ODATA_PASSWORD", "")) if odata_user else None,
        exchange_name=os.environ["CDC1C_EXCHANGE_NAME"],
        queue_guid=os.environ["CDC1C_QUEUE_GUID"],
        engine=engine,
        db_schema=os.environ.get("CDC1C_DB_SCHEMA"),
        full_load_workers=full_load_workers,
    )

    # Уровень логирования — после конструктора: он вешает обработчик на логгер cdc_1c
    # (по умолчанию INFO), а тут переопределяем на заданный (например, DEBUG/WARNING).
    logging.getLogger("cdc_1c").setLevel(os.environ.get("CDC1C_LOG_LEVEL", "INFO").upper())

    mode = os.environ.get("CDC1C_MODE", "loop")
    if mode == "once":
        replicator.run_once()
    elif mode == "loop":
        replicator.run_forever(interval=float(os.environ.get("CDC1C_POLL_INTERVAL", "60")))
    else:
        raise SystemExit(f"Unknown CDC1C_MODE={mode!r} (expected 'loop' or 'once')")


if __name__ == "__main__":
    main()
