import logging
import signal
import time

from sqlalchemy import Engine, create_engine

from cdc_1c.metadata_reader import MetadataReader1C
from cdc_1c.change_reader import ChangeReader1C
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.db_writer import DBWriter1C
from cdc_1c.config import Config
from cdc_1c.logging_config import _ensure_handler

logger = logging.getLogger(__name__)


class Replicator1C:
    """
    Оркестратор CDC: читает изменения из 1С (OData) и сохраняет их в БД, подтверждая получение
    только после успешного сохранения.

    Компоненты (MetadataReader1C / ChangeReader1C / NameMapper1C / DBWriter1C) строятся в
    конструкторе, но без обращения к сети: MetadataReader1C создаётся пустым, а фактическая
    загрузка метаданных (сетевой запрос) откладывается до первого run_once (по флагу
    metadata.is_loaded). Поэтому недоступность 1С на старте не роняет конструктор — ошибка загрузки
    всплывает уже в run_once: в run_forever она попадает в его try/except и повторяется, а в
    одиночном run_once пробрасывается (это нормально).

    Принимает отдельные аргументы (а не объект Config), чтобы библиотечный вызов был прямым и
    обходился без обёртки. БД передаётся готовым engine — пользователь сам управляет пулом/опциями,
    а тот же engine прокидывается в DBWriter1C. Для запуска из env есть classmethod from_config.
    """

    def __init__(self, odata_url: str, odata_user: str | None, odata_password: str | None,
                 exchange_name: str, queue_guid: str,
                 engine: Engine, db_schema: str | None = None,
                 request_timeout: float | None = None):
        # Включаем вывод логов, если приложение не настроило логирование само.
        _ensure_handler()

        self.engine = engine
        self.db_schema = db_schema
        self._odata_url = odata_url
        self._exchange_name = exchange_name
        self._queue_guid = queue_guid
        self._auth = (odata_user, odata_password) if odata_user is not None else None
        # None → таймаут не задан явно: run_forever возьмёт его равным interval; одиночный
        # run_once без явного значения работает без таймаута (поведение requests по умолчанию).
        self._request_timeout = request_timeout

        # Компоненты строятся сразу, но в сеть не ходят: MetadataReader1C создаётся пустым,
        # метаданные подгрузятся лениво при первом run_once.
        self.metadata = MetadataReader1C(self._odata_url, auth=self._auth,
                                         request_timeout=self._request_timeout)
        self.name_mapper = NameMapper1C()
        self.changes = ChangeReader1C(self._odata_url, self._exchange_name, self._queue_guid,
                                      self.metadata, auth=self._auth,
                                      request_timeout=self._request_timeout)
        self.writer = DBWriter1C(engine=self.engine, name_mapper=self.name_mapper,
                                 data_reader=self.changes, schema=self.db_schema)


    @classmethod
    def from_config(cls, config: Config) -> "Replicator1C":
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

    def run_once(self, notify_changes: bool = True) -> None:
        """
        Один цикл: (load metadata при первом вызове) → read → save → notify. Подтверждение
        получения отправляется только после успешного сохранения — если save_all упадёт, изменения
        не подтверждаются и придут снова.

        Метаданные грузятся при первом вызове (первый сетевой запрос). В run_forever его падение
        ловится и повторяется; в одиночном run_once — пробрасывается.

        Если изменений не было, notify не шлём: незачем подтверждать и двигать счётчик пакета
        обмена на пустом пакете.

        notify_changes=False отключает подтверждение совсем: изменения остаются в очереди обмена
        1С (полезно для отладки/тестов — цикл становится повторяемым).
        """
        # Первый вызов: грузим метаданные. Дальше не перечитываем — это делает сам data_reader
        # при появлении нового объекта/поля (get_metadata держит is_loaded=True).
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()
        self.changes.read_changes()
        self.writer.save_all()
        if notify_changes and len(self.changes) > 0:
            self.changes.notify_changes_received()
        else:
            logger.debug("No changes — skipping confirmation")

    def run_forever(self, interval: float = 60.0, max_iterations: int = 0) -> None:
        """
        Цикл run_once с паузой interval секунд. Упавший цикл логируется и не подтверждается —
        повтор на следующей итерации. Корректно завершается по SIGTERM/SIGINT.

        max_iterations ограничивает число итераций (0 — бесконечно). Итерацией считается каждый
        вызов run_once, включая упавший на коннекте: ретрай подключения к недоступной 1С — это
        и есть итерация (внутри run_once своих ретраев нет), поэтому max_iterations ограничивает
        и число попыток подключения. Полезно для отладки/тестов.

        Если таймаут запросов явно не задан в конструкторе, он берётся равным interval: запрос не
        должен висеть дольше периода опроса (иначе зависший сервер не даст циклу дойти до ретрая).
        """
        # Таймаут запросов по умолчанию = период опроса (см. докстринг).
        if self._request_timeout is None:
            self._request_timeout = interval

        stop = _StopSignal()
        logger.info("Starting replication loop (interval=%ss, max_iterations=%s, timeout=%ss)",
                    interval, max_iterations, self._request_timeout)
        iterations = 0
        while not stop.requested:
            try:
                self.run_once()
            except Exception:
                logger.exception("Replication cycle failed, will retry")

            iterations += 1
            if max_iterations > 0 and iterations >= max_iterations:
                logger.info("Reached max_iterations (%s), stopping", max_iterations)
                break

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
