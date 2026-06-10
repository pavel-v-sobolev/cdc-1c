import os
from dataclasses import dataclass


@dataclass
class Config:
    """
    Настройки оркестратора. Ядро не зависит от окружения: Config заполняется явно (библиотечный
    сценарий) либо через from_env() (entrypoint контейнера). Строку подключения к БД (db_url)
    в engine превращает Replicator1C.from_config.
    """

    odata_url: str
    odata_user: str | None
    odata_password: str | None
    exchange_name: str
    queue_guid: str
    db_url: str
    db_schema: str | None = None
    poll_interval: float = 60.0
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        """Читает настройки из переменных окружения CDC1C_* (используется только entrypoint-ом)."""
        return cls(
            odata_url=os.environ["CDC1C_ODATA_URL"],
            odata_user=os.environ.get("CDC1C_ODATA_USER"),
            odata_password=os.environ.get("CDC1C_ODATA_PASSWORD"),
            exchange_name=os.environ["CDC1C_EXCHANGE_NAME"],
            queue_guid=os.environ["CDC1C_QUEUE_GUID"],
            db_url=os.environ["CDC1C_DB_URL"],
            db_schema=os.environ.get("CDC1C_DB_SCHEMA"),
            poll_interval=float(os.environ.get("CDC1C_POLL_INTERVAL", "60")),
            log_level=os.environ.get("CDC1C_LOG_LEVEL", "INFO"),
        )
