import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Engine, create_engine

from cdc_1c.metadata_reader import MetadataReader1C
from cdc_1c.data_reader import DataReader1C
from cdc_1c.change_reader import ChangeReader1C
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.db_writer import DBWriter1C, save_order_key
from cdc_1c.config import Config
from cdc_1c.db_logs import Replicator1CLog
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

    def __init__(self, odata_url: str, odata_auth: tuple[str, str] | None,
                 exchange_name: str, queue_guid: str,
                 engine: Engine, db_schema: str | None = None,
                 request_timeout: float | None = None,
                 full_load_workers: int = 2):
        # Включаем вывод логов, если приложение не настроило логирование само.
        _ensure_handler()

        self.engine = engine
        self.db_schema = db_schema
        self._odata_url = odata_url
        self._exchange_name = exchange_name
        self._queue_guid = queue_guid
        # odata_auth — кортеж (user, password) либо None, как в ридерах (передаётся им как есть).
        self._odata_auth = odata_auth
        # None → таймаут не задан явно: run_forever возьмёт его равным interval; одиночный
        # run_once без явного значения работает без таймаута (поведение requests по умолчанию).
        self._request_timeout = request_timeout

        # Компоненты строятся сразу, но в сеть не ходят: MetadataReader1C создаётся пустым,
        # метаданные подгрузятся лениво при первом run_once. MetadataReader1C получает engine —
        # он же ведёт реестр объектов metadata_objects_1c (состояние полной выгрузки).
        self.metadata = MetadataReader1C(self._odata_url, odata_auth=self._odata_auth,
                                         request_timeout=self._request_timeout,
                                         engine=self.engine, schema=self.db_schema)
        self.name_mapper = NameMapper1C()

        # Фоновая полная выгрузка: пул потоков и защита от повторного сабмита одного объекта.
        self._full_load_workers = full_load_workers
        self._full_load_in_progress: set[str] = set()
        self._in_progress_lock = threading.Lock()
        self.changes = ChangeReader1C(self._odata_url, self._exchange_name, self._queue_guid,
                                      self.metadata, odata_auth=self._odata_auth,
                                      request_timeout=self._request_timeout)
        self.writer = DBWriter1C(engine=self.engine, name_mapper=self.name_mapper,
                                 data_reader=self.changes, schema=self.db_schema)
        # Лог загрузки (строка на объект) пишет оркестратор: только здесь есть контекст обмена
        # (exchange_name/message_no), а writer универсален и может делать и полную перевыгрузку.
        self.replicator_log = Replicator1CLog(self.engine, self.db_schema)


    @classmethod
    def from_config(cls, config: Config) -> "Replicator1C":
        """Собирает оркестратор из Config: строит engine из db_url и маппит остальные поля."""
        engine = create_engine(config.db_url)
        odata_auth = (config.odata_user, config.odata_password) if config.odata_user is not None else None
        return cls(
            odata_url=config.odata_url,
            odata_auth=odata_auth,
            exchange_name=config.exchange_name,
            queue_guid=config.queue_guid,
            engine=engine,
            db_schema=config.db_schema,
            full_load_workers=config.full_load_workers,
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
        self._save_changes()
        # Объект пришёл в пакете → он в плане обмена. Если ни разу не выгружался целиком,
        # помечаем на полную выгрузку (выполнит фоновый воркер в run_forever). Табличные части
        # (object_key == ['Ref_Key']) пропускаем — у них нет отдельной OData-сущности, они
        # догружаются вместе с владельцем при его full_load.
        for object_name in self.changes:
            metadata_obj = self.metadata.get(object_name)
            if metadata_obj is not None and metadata_obj.object_key == ['Ref_Key']:
                continue
            self.metadata.require_full_load_if_new(object_name)
        if notify_changes and len(self.changes) > 0:
            self.changes.notify_changes_received()
        else:
            logger.debug("No changes — skipping confirmation")

    def _save_changes(self) -> None:
        """
        Сохраняет объекты пакета по одному, записывая лог загрузки на каждый объект
        (replicator_1c_log). finish() — только после успешного save: упавший объект остаётся с
        finished_at=NULL и не двигает границу обработки материализатора. Лог здесь, а не в DBWriter1C,
        потому что только тут есть контекст обмена (exchange_name/message_no).

        Порядок сохранения — справочники → документы → регистры (save_order_key): документы ссылаются
        на справочники, регистры — на документы, поэтому родителей пишем раньше. Внутри группы
        исходный порядок пакета (сортировка стабильна).
        """
        for object_name, data_object in sorted(self.changes.items(),
                                               key=lambda kv: save_order_key(kv[0])):
            log_id = self.replicator_log.start(
                self.changes.exchange_name, object_name, self.changes.message_no)
            self.writer.save(object_name, data_object)
            self.replicator_log.finish(log_id)

    def list_objects(self) -> list[str]:
        """
        Список имён объектов 1С, доступных для выгрузки (ключи метаданных: документы/справочники и
        регистры). Метаданные при необходимости подгружаются (первый сетевой запрос). Удобно, чтобы
        узнать, что передавать в full_load.
        """
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()
        return list(self.metadata.keys())

    def full_load(self, object_name: str, batch_size: int = 1000) -> None:
        """
        Полная постраничная выгрузка объекта 1С в целевую таблицу: по batch_size записей за запрос,
        каждая страница сразу сохраняется upsert-ом (без удаления). Идемпотентно — повторный прогон
        обновляет строки по ключу. Удаления не делаются: при постраничном чтении нельзя отличить
        «строки нет в источнике» от «строка на другой странице», поэтому writer.save(delete=False).

        Документ/справочник выгружается вместе с табличными частями — они приходят вложенно в той же
        странице и сохраняются как отдельные объекты. Страницы берутся keyset-пагинацией (фильтр
        «ключ больше последнего значения» вместо $skip — без перечитывания пропущенных строк на
        больших объёмах): справочник/документ — по Ref_Key (guid), регистр — по Recorder (строка,
        одна entry = целый набор записей регистратора). Один прогон = одна строка в replicator_1c_log
        (message_no=NULL — это не пакет обмена); finished_at проставляется после успеха всех страниц.
        """
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()

        # Ключ курсора: справочник/документ → Ref_Key (guid-литерал), регистр → Recorder (строковый).
        key_field, key_is_guid = self._full_load_key(object_name)

        reader = DataReader1C(self._odata_url, self.metadata, odata_auth=self._odata_auth,
                              request_timeout=self._request_timeout)

        log_id = self.replicator_log.start(self._exchange_name, object_name, None)
        logger.info("Full load of %s started (batch_size=%s, key=%s)",
                    object_name, batch_size, key_field)
        last_key = None
        total = 0
        while True:
            page = reader.read_object(object_name, top=batch_size, key_field=key_field,
                                      after_key=last_key, key_is_guid=key_is_guid)
            for obj_name, data_object in reader.items():
                self.writer.save(obj_name, data_object, delete=False)
            total += page
            if page < batch_size:
                break
            # Курсор следующей страницы — ключ последней записи (порядок по ключу возрастающий).
            last_key = reader[object_name].data[key_field][-1]
        self.replicator_log.finish(log_id)
        logger.info("Full load of %s finished (%s records)", object_name, total)

    def _full_load_key(self, object_name: str) -> tuple[str, bool]:
        """
        Поле и тип литерала для keyset-курсора full_load по метаданным объекта:
        - Ref_Key (справочник/документ) → ('Ref_Key', guid-литерал);
        - Recorder (регистр) → ('Recorder', строковый литерал; Recorder в OData отдаётся строкой).
        Иначе keyset неприменим (нет одиночного ключа курсора) → ValueError.
        """
        metadata_obj = self.metadata.get(object_name)
        primary_key = metadata_obj.primary_key if metadata_obj else {}
        if 'Ref_Key' in primary_key:
            return 'Ref_Key', True
        if 'Recorder' in primary_key:
            return 'Recorder', False
        raise ValueError(
            f"full_load: no keyset cursor for {object_name} (need Ref_Key or Recorder in key)")

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

        После каждого цикла фоном (пул потоков) запускаются полные выгрузки помеченных объектов —
        диспетчеризация только здесь (одиночный run_once лишь взводит флаги).
        """
        # Таймаут запросов по умолчанию = период опроса (см. докстринг). interval=0 (без пауз,
        # обычно в тестах) таймаутом быть не может — оставляем None (ридеры подставят дефолт).
        # Ридеры metadata/changes уже построены в __init__ со старым значением, поэтому обновляем
        # их .request_timeout явно — иначе переопределение «= interval» до них не доходит.
        if self._request_timeout is None and interval > 0:
            self._request_timeout = interval
            self.metadata.request_timeout = self._request_timeout
            self.changes.request_timeout = self._request_timeout

        stop = _StopSignal()
        logger.info("Starting replication loop (interval=%ss, max_iterations=%s, timeout=%ss)",
                    interval, max_iterations, self._request_timeout)
        iterations = 0
        with ThreadPoolExecutor(max_workers=self._full_load_workers,
                                thread_name_prefix='full_load') as executor:
            while not stop.requested:
                try:
                    self.run_once()
                    self._dispatch_full_loads(executor)
                except Exception:
                    logger.exception("Replication cycle failed, will retry")

                iterations += 1
                if max_iterations > 0 and iterations >= max_iterations:
                    logger.info("Reached max_iterations (%s), stopping", max_iterations)
                    break

                stop.wait(interval)
            logger.info("Replication loop stopping, waiting for full loads to finish")
        logger.info("Replication loop stopped")

    def _dispatch_full_loads(self, executor: ThreadPoolExecutor) -> None:
        """
        Ставит в пул полные выгрузки объектов с full_load_is_required, кроме уже выполняющихся.
        Защита от повторного сабмита — множество _full_load_in_progress (под локом).
        """
        for object_name in self.metadata.list_full_load_required():
            with self._in_progress_lock:
                if object_name in self._full_load_in_progress:
                    continue
                self._full_load_in_progress.add(object_name)
            executor.submit(self._run_full_load, object_name)

    def _run_full_load(self, object_name: str) -> None:
        """Фоновая полная выгрузка одного объекта; на успехе фиксирует mark_full_loaded.
        При ошибке флаг full_load_is_required остаётся → ретрай на следующем цикле."""
        try:
            self.full_load(object_name)
            self.metadata.mark_full_loaded(object_name)
        except Exception:
            logger.exception("Background full_load of %s failed, will retry", object_name)
        finally:
            with self._in_progress_lock:
                self._full_load_in_progress.discard(object_name)


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
