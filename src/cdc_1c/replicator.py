import logging
import signal
import time
from typing import TYPE_CHECKING

from sqlalchemy import Engine, create_engine

from cdc_1c.metadata_reader import MetadataReader1C
from cdc_1c.change_reader import ChangeReader1C
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.db_writer import DBWriter1C
from cdc_1c.logging_config import _ensure_handler

if TYPE_CHECKING:
    from cdc_1c.config import Config

logger = logging.getLogger(__name__)


class Replicator1C:
    """
    Оркестратор CDC: читает изменения из 1С (OData) и сохраняет их в БД, подтверждая получение
    только после успешного сохранения.

    Компоненты (MetadataReader1C / ChangeReader1C / NameMapper1C / DBWriter1C) собираются один раз
    в конструкторе; метаданные читаются сразу, изменения — на каждый цикл (read_changes сам чистит
    накопленное состояние).

    Принимает отдельные аргументы (а не объект Config), чтобы библиотечный вызов был прямым и
    обходился без обёртки. БД передаётся готовым engine — пользователь сам управляет пулом/опциями,
    а тот же engine прокидывается в DBWriter1C. Для запуска из env есть classmethod from_config.
    """

    def __init__(self, odata_url: str, odata_user: str | None, odata_password: str | None,
                 exchange_name: str, queue_guid: str,
                 engine: Engine, db_schema: str | None = None):
        # Включаем вывод логов, если приложение не настроило логирование само.
        _ensure_handler()

        self.engine = engine
        self.db_schema = db_schema

        auth = (odata_user, odata_password) if odata_user is not None else None

        self.metadata = MetadataReader1C(odata_url, auth=auth)
        self.name_mapper = NameMapper1C()
        self.changes = ChangeReader1C(odata_url, exchange_name, queue_guid,
                                      self.metadata, auth=auth)
        self.writer = DBWriter1C(engine=engine, name_mapper=self.name_mapper,
                                 data_reader=self.changes, schema=db_schema)

    @classmethod
    def from_config(cls, config: "Config") -> "Replicator1C":
        """Собирает оркестратор из Config: строит engine из db_url и маппит остальные поля."""
        engine = create_engine(config.db_url)
        return cls(
            odata_url=config.odata_url,
            odata_user=config.odata_user,
            odata_password=config.odata_password,
            exchange_name=config.exchange_name,
            queue_guid=config.queue_guid,
            engine=engine,
            db_schema=config.db_schema,
        )

    def run_once(self) -> None:
        """
        Один цикл: read → save → notify. Подтверждение получения отправляется только после
        успешного сохранения — если save_all упадёт, изменения не подтверждаются и придут снова.
        """
        logger.info("Reading changes from 1C")
        self.changes.read_changes()
        self.writer.save_all()
        self.changes.notify_changes_received()

    def run_forever(self, interval: float = 60.0) -> None:
        """
        Бесконечный цикл run_once с паузой interval секунд. Упавший цикл логируется и не
        подтверждается — повтор на следующей итерации. Корректно завершается по SIGTERM/SIGINT.
        """
        stop = _StopSignal()
        logger.info("Starting replication loop (interval=%ss)", interval)
        while not stop.requested:
            try:
                self.run_once()
            except Exception:
                logger.exception("Replication cycle failed, will retry")
            stop.wait(interval)
        logger.info("Replication loop stopped")


class _StopSignal:
    """Перехватывает SIGTERM/SIGINT для graceful-остановки run_forever."""

    def __init__(self):
        self.requested = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, frame):
        logger.info("Received signal %s, stopping after current cycle", signum)
        self.requested = True

    def wait(self, seconds: float) -> None:
        """Спит до seconds, прерываясь раньше при поступлении сигнала."""
        deadline = time.monotonic() + seconds
        while not self.requested and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
