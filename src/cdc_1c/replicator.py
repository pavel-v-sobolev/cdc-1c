import functools
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import requests
from sqlalchemy import Engine, create_engine

from cdc_1c.metadata_reader import MetadataReader1C
from cdc_1c.common_functions import format_duration
from cdc_1c.data_reader import DataReader1C, RECORDER_FIELDS
from cdc_1c.change_reader import ChangeReader1C
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.db_writer import DBWriter1C, save_order_key
from cdc_1c.config import Config
from cdc_1c.db_logs import Replicator1CLog, LOAD_TYPE_CHANGES, LOAD_TYPE_FULL
from cdc_1c.logging_config import _ensure_handler, get_logger, load_mode, LOAD_MODE_CHANGES, LOAD_MODE_FULL

logger = get_logger(__name__)

# Ретрай упавшего цикла (run_forever): экспоненциальная пауза вместо слепого повтора каждые
# interval секунд. Неудачный SelectChanges — это не бесплатная попытка: 1С успевает отработать
# минуты на таблице регистрации изменений, и повторы начинают накладываться друг на друга,
# порождая уже конфликты блокировок. Пауза удваивается до потолка и сбрасывается после успеха.
BACKOFF_FACTOR = 2.0
DEFAULT_MAX_BACKOFF = 1800.0

# HTTP-коды, при которых повтор того же запроса бессмысленен: права, адрес, состав запроса.
# Такие ошибки сразу уводят паузу на потолок — процесс живёт (перезапуск ничего не чинит),
# но 1С не долбим. Всё остальное (таймаут, обрыв, 5xx, конфликт блокировок) считаем временным.
PERMANENT_HTTP_CODES = frozenset((400, 401, 403, 404, 405, 501))

# Полная выгрузка: во сколько раз уменьшать страницу, если 1С не осилила запрос, и нижний предел.
# Страницу объекта 1С собирает целиком во временных файлах на сервере приложений, а её объём
# зависит не от batch_size, а от того, сколько строк тянется вместе с одной записью: у документа
# с табличными частями entry — это сотни строк, у регистраторного регистра — весь набор движений
# регистратора, а он бывает и в мегабайт. Универсального batch_size поэтому нет: при отказе
# уменьшаем страницу и повторяем, вплоть до одной записи за запрос — меньше уже некуда, entry
# неделима. Найденный размер запоминается на объект (_full_load_page_size), чтобы повторный
# прогон не начинал снова с batch_size и не жёг сервер заведомо провальными попытками.
FULL_LOAD_BATCH_DIVISOR = 4
FULL_LOAD_MIN_BATCH = 1

# Целевой вес страницы полной выгрузки и размер первой («пробной») страницы, пока вес entry
# неизвестен. Размер страницы подбирается по факту: после каждой страницы известен её вес и
# число entry, отсюда — сколько entry укладывается в бюджет. batch_size остаётся верхней
# границей. Просить у 1С сразу batch_size нельзя: у толстого объекта это гигабайты временных
# файлов на сервере приложений, и запрос падает ещё до того, как мы узнаем вес entry.
FULL_LOAD_TARGET_BYTES = 32 * 1024 * 1024
FULL_LOAD_PROBE_BATCH = 20


def _is_permanent_error(exc: BaseException) -> bool:
    """Ошибка, которую ретрай не исправит (см. PERMANENT_HTTP_CODES)."""
    response = getattr(exc, 'response', None)
    status = getattr(response, 'status_code', None)
    return status in PERMANENT_HTTP_CODES


def _log_failure(exc: BaseException, message: str, *args) -> None:
    """
    Пишет в лог падение цикла. Для ошибок обмена traceback не нужен: он целиком состоит из
    внутренностей requests и ничего не добавляет к описанию, которое пришло от 1С.

    - HTTPError: описание уже выведено raise_for_status строкой выше, не дублируем;
    - прочие ошибки requests (таймаут, обрыв): traceback не нужен, но текст выводим — его нигде нет;
    - остальное: это уже похоже на ошибку в коде, traceback оставляем.
    """
    if isinstance(exc, requests.HTTPError):
        logger.error(message, *args)
    elif isinstance(exc, requests.RequestException):
        logger.error(f'{message}: %s', *args, exc)
    else:
        logger.exception(message, *args)


def _load_mode_tag(mode: str):
    """
    Помечает режимом загрузки все сообщения лога cdc_1c, выданные внутри метода: полная выгрузка
    идёт фоновыми потоками параллельно с чтением изменений, и в общем логе иначе не разобрать, к
    чему относится строка. Декоратором, а не блоком with — чтобы не заворачивать тело целиком.
    """
    def decorator(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            with load_mode(mode):
                return method(self, *args, **kwargs)
        return wrapper
    return decorator


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
        # None → таймаут не задан явно: ридеры подставят DEFAULT_REQUEST_TIMEOUT (metadata_reader).
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
        # Размер страницы, который 1С реально осилила по этому объекту (см. FULL_LOAD_MIN_BATCH).
        # Пишет только поток самой выгрузки, а он на объект один (_full_load_in_progress).
        self._full_load_page_size: dict[str, int] = {}
        self._in_progress_lock = threading.Lock()
        self.changes = ChangeReader1C(self._odata_url, self._exchange_name, self._queue_guid,
                                      self.metadata, odata_auth=self._odata_auth,
                                      request_timeout=self._request_timeout)
        self.writer = DBWriter1C(engine=self.engine, name_mapper=self.name_mapper,
                                 schema=self.db_schema)
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

    @_load_mode_tag(LOAD_MODE_CHANGES)
    def run_once(self, notify_changes: bool = True) -> None:
        """
        Один цикл: (load metadata при первом вызове) → read → save → notify. Подтверждение
        получения отправляется только после успешного сохранения — если save упадёт, изменения
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
        # Время пакета считаем от чтения из 1С и до конца всех merge — это то, что реально
        # занимает цикл. Загрузка метаданных сюда не входит: она разовая и к пакету не относится.
        started = time.monotonic()
        self.changes.read_changes()
        self._save_changes()
        logger.info("Changes package %s processed in %s: %s rows",
                    self.changes.message_no, format_duration(time.monotonic() - started),
                    self.changes.rows_read())
        # Объект пришёл в пакете → он в плане обмена. Если ни разу не выгружался целиком,
        # помечаем на полную выгрузку (выполнит фоновый воркер в run_forever). 
        # Табличные части пропускаем — у них нет отдельной OData-сущности, они
        # догружаются вместе с владельцем при его full_load.
        for object_full_name in self.changes:
            metadata_obj = self.metadata.get(object_full_name)
            if metadata_obj is None:
                self.metadata.get_metadata()
                metadata_obj = self.metadata.get(object_full_name)
                if metadata_obj is None:
                    raise RuntimeError(f"No metadata object for {object_full_name}")

            if metadata_obj.is_table_part:
                continue

            self.metadata.require_full_load_if_new(object_full_name)
        
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
                self.changes.exchange_name, object_name, self.changes.message_no, LOAD_TYPE_CHANGES)
            result = self.writer.save(object_name, data_object)
            # Одно сохранение на строку лога: счётчики и завершение — одним запросом.
            self.replicator_log.write_result(log_id, result, finish=True)

    def list_objects(self) -> list[str]:
        """
        Список имён объектов 1С, доступных для выгрузки (документы/справочники и регистры). Табличные
        части исключаются — у них нет отдельной OData-сущности, их нельзя выгрузить напрямую (они
        приходят вложенно с владельцем). Метаданные при необходимости подгружаются (первый сетевой
        запрос). Удобно, чтобы узнать, что передавать в full_load.
        """
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()
        return [name for name, obj in self.metadata.items() if not obj.is_table_part]

    @_load_mode_tag(LOAD_MODE_FULL)
    def full_load(self, object_name: str, batch_size: int = 1000,
                  date_field: str | None = None,
                  date_from: date | datetime | str | None = None,
                  date_to: date | datetime | str | None = None) -> None:
        """
        Полная постраничная выгрузка объекта 1С в целевую таблицу: по batch_size записей за запрос,
        каждая страница сразу сохраняется через writer.save(full_load=True). Идемпотентно — повторный
        прогон обновляет строки по ключу. Документ/справочник — чистый upsert; регистр/табличная часть —
        own-or-skip группы целиком (группа умещается на одной странице), см. DBWriter1C.save.

        Документ/справочник выгружается вместе с табличными частями — они приходят вложенно в той же
        странице и сохраняются как отдельные объекты. Сортировка страниц — по первичному ключу
        (см. _full_load_key), а способ перехода к следующей странице зависит от ключа: keyset-фильтр
        «ключ больше последней строки» там, где он корректен, иначе $skip (см. _supports_keyset).
        Один прогон = одна строка в replicator_1c_log (message_no=NULL — это не пакет обмена);
        finished_at проставляется после успеха всех страниц.

        batch_size — верхняя граница, а не жёсткий размер. Реальный размер страницы подбирается по
        её весу (см. _next_page_size): первая страница пробная, дальше столько записей, сколько
        укладывается в FULL_LOAD_TARGET_BYTES. Если 1С всё же не осилила страницу (500), размер
        уменьшается и запрос повторяется с того же места (см. FULL_LOAD_BATCH_DIVISOR).

        Версия и гонка с изменениями: строки выгрузки штампуются exchange_message_no=0, а save идёт с
        full_load=True — с version-guard'ами по emn, чтобы устаревший снимок не затирал более свежие
        изменения (emn>0) и не воскрешал удалённые строки групп (регистр/ТЧ). См. DBWriter1C.save.

        Необязательный фильтр по периоду: date_field — имя поля даты/времени объекта (Date у
        документов, Period у регистров), date_from/date_to — границы (datetime/date/ISO-строка,
        включительно). Транслируется в OData $filter `date_field ge …[ and date_field le …]` и
        объединяется с keyset-курсором по AND. Полезно для ручной догрузки за нужный период.
        """
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()

        # Ключ курсора: справочник/документ → [Ref_Key], регистраторный → [Recorder]/[Recorder_Key],
        # независимый регистр → весь первичный ключ (составной ключ).
        key_fields, key_types = self._full_load_key(object_name)
        use_keyset = self._supports_keyset(object_name, key_fields)
        date_filter = self._build_date_filter(date_field, date_from, date_to)

        reader = DataReader1C(self._odata_url, self.metadata, odata_auth=self._odata_auth,
                              request_timeout=self._request_timeout)
        # Полная выгрузка = базовая версия: emn=0 (ниже любого номера пакета изменений >=1).
        reader.exchange_message_no = 0

        log_id = self.replicator_log.start(self._exchange_name, object_name, None, LOAD_TYPE_FULL)
        logger.info("Full load of %s started (batch_size=%s, key=%s, paging=%s, date_filter=%s)",
                    object_name, batch_size, key_fields,
                    'keyset' if use_keyset else 'skip', date_filter)
        after_values = None
        skip = 0
        total = 0
        # Начинаем с размера, подобранного по этому объекту раньше, иначе — с пробной страницы.
        page_size = min(batch_size,
                        self._full_load_page_size.get(object_name, FULL_LOAD_PROBE_BATCH))
        while True:
            try:
                page = reader.read_object(object_name, top=page_size, key_fields=key_fields,
                                          after_values=after_values, key_types=key_types,
                                          extra_filter=date_filter,
                                          skip=None if use_keyset else skip)
            except requests.HTTPError as exc:
                # Страница не по зубам серверу 1С (упирается в память/временные файлы) —
                # уменьшаем её и повторяем с того же места. Курсор/смещение не сдвигались.
                if _is_permanent_error(exc) or page_size <= FULL_LOAD_MIN_BATCH:
                    raise
                page_size = max(FULL_LOAD_MIN_BATCH, page_size // FULL_LOAD_BATCH_DIVISOR)
                self._full_load_page_size[object_name] = page_size
                logger.warning("Full load of %s: page failed, retrying with batch_size=%s",
                               object_name, page_size)
                continue
            for obj_name, data_object in reader.items():
                # Много страниц/объектов пишутся в одну строку лога — счётчики суммируются в БД.
                result = self.writer.save(obj_name, data_object, full_load=True)
                self.replicator_log.write_result(log_id, result)
            total += page
            if page < page_size:
                break
            if use_keyset:
                # Курсор следующей страницы — значения ключевых полей последней записи.
                data = reader[object_name].data
                after_values = [data[f][-1] for f in key_fields]
            else:
                skip += page
            page_size = self._next_page_size(object_name, page_size, page,
                                             reader.last_response_bytes, batch_size)
        self.replicator_log.write_result(log_id, finish=True)
        logger.info("Full load of %s finished (%s records)", object_name, total)

    @staticmethod
    def _odata_datetime(value: date | datetime | str) -> str:
        """OData-литерал datetime'YYYY-MM-DDTHH:MM:SS' из datetime/date (date → полночь) или строки.
        1С хранит дату-время с точностью до секунды (без миллисекунд), поэтому формат — до секунд;
        доли секунды у переданного datetime усекаются (для 1С безопасно)."""
        if isinstance(value, (datetime, date)):
            value = value.strftime('%Y-%m-%dT%H:%M:%S')
        return f"datetime'{value}'"

    @staticmethod
    def _build_date_filter(date_field: str | None,
                           date_from: date | datetime | str | None,
                           date_to: date | datetime | str | None) -> str | None:
        """
        OData $filter по периоду (границы включительно). Возвращает None, если границы не заданы;
        требует date_field, если задана хотя бы одна граница.

        Верхняя граница date_to:
        - чистая дата (date без времени) → включаем весь день целиком, даже если поле хранит
          дату-время: `date_field lt <дата+1 день, полночь>` (иначе `le 2026-06-30T00:00:00`
          отсекло бы все записи этого дня, кроме полуночи);
        - дата-время или строка → используем как есть: `date_field le <to>`.
        Нижняя граница date_from всегда включительна (`ge`); чистая дата = с начала дня (полночь).
        """
        if date_from is None and date_to is None:
            return None
        if not date_field:
            raise ValueError("full_load: date_from/date_to require date_field")
        clauses = []
        if date_from is not None:
            clauses.append(f"{date_field} ge {Replicator1C._odata_datetime(date_from)}")
        if date_to is not None:
            if isinstance(date_to, date) and not isinstance(date_to, datetime):
                next_day = date_to + timedelta(days=1)
                clauses.append(f"{date_field} lt {Replicator1C._odata_datetime(next_day)}")
            else:
                clauses.append(f"{date_field} le {Replicator1C._odata_datetime(date_to)}")
        return " and ".join(clauses)

    def _next_page_size(self, object_name: str, page_size: int, entries: int,
                        response_bytes: int, batch_size: int) -> int:
        """
        Размер следующей страницы по фактическому весу выданной: сколько entry укладывается в
        FULL_LOAD_TARGET_BYTES. Вес entry у разных объектов различается на порядки (строка
        справочника — килобайты, документ с табличными частями или набор движений регистратора —
        мегабайты), поэтому единый batch_size либо гоняет лишние запросы, либо просит у 1С
        страницу в гигабайты. batch_size — верхняя граница, FULL_LOAD_MIN_BATCH — нижняя.
        """
        if not entries or not response_bytes:
            return page_size
        per_entry = response_bytes / entries
        fits = max(FULL_LOAD_MIN_BATCH, int(FULL_LOAD_TARGET_BYTES / per_entry))
        page_size = min(batch_size, fits)
        self._full_load_page_size[object_name] = page_size
        return page_size

    def _supports_keyset(self, object_name: str, key_fields: list[str]) -> bool:
        """
        Можно ли листать объект keyset-курсором (фильтр «ключ больше последней строки») — или
        придётся платить за $skip. Ссылочное поле в ключе запрещает keyset по двум причинам:

        - сравнение: `Ref_Key gt guid'...'` 1С отдаёт 500 «Нельзя сравнивать поля неограниченной
          длины и поля несовместимых типов» (в запрос уходит `sourceAlias.Ref > &param`). Строковый
          литерал 500 не даёт, но сравнивает не то, что нужно;
        - сортировка: `$orderby` по ссылке 1С разворачивает в АВТОУПОРЯДОЧИВАНИЕ — сортирует по
          полям представления объекта (наименование/код у справочника, дата+номер у документа),
          а не по GUID. Курсор «GUID больше предыдущего» такой порядок не продолжает: страницы
          пересекаются и теряют строки.

        Ссылочность определяем по типу из метаданных (Guid) и по имени поля — `Ref_Key`,
        `Recorder`/`Recorder_Key`, измерения `*_Key`. Имя нужно потому, что часть ссылочных полей
        1С описывает в $metadata как String (это и чинит GUESS_UUID_TYPES, но флаг отключаемый).
        Period/LineNumber и прочие скаляры ссылками не считаются — по ним keyset корректен.
        """
        properties = self.metadata.get(object_name) or {}
        return not any(properties.get(field) == 'Guid' or field.endswith('_Key')
                       or field in RECORDER_FIELDS for field in key_fields)

    def _full_load_key(self, object_name: str) -> tuple[list[str], list[str]]:
        """
        Поля и их типы для keyset-курсора full_load по метаданным объекта (списки — для составного
        ключа, см. read_object):
        - Ref_Key (справочник/документ) → (['Ref_Key'], ['Guid']);
        - Recorder (регистраторный регистр) → (['Recorder'], ['String']; Recorder в OData отдаётся
          строкой, одна entry = набор регистратора);
        - Recorder_Key (тот же регистраторный регистр, но с единственным типом регистратора — 1С
          отдаёт поле как Guid и без Recorder_Type) → (['Recorder_Key'], ['Guid']);
        - иначе (независимый регистр сведений) → весь первичный ключ: (список полей, список типов) —
          составной лексикографический keyset, т.к. одиночного уникального курсора нет.
        Пустой первичный ключ → ValueError.
        """
        metadata_obj = self.metadata.get(object_name)
        primary_key = metadata_obj.primary_key if metadata_obj else {}
        if 'Ref_Key' in primary_key:
            return ['Ref_Key'], ['Guid']
        if 'Recorder' in primary_key:
            return ['Recorder'], ['String']
        if 'Recorder_Key' in primary_key:
            return ['Recorder_Key'], ['Guid']
        if primary_key:
            return list(primary_key.keys()), list(primary_key.values())
        raise ValueError(f"full_load: no primary key for {object_name}")

    def run_forever(self, interval: float = 60.0, max_iterations: int = 0,
                    max_backoff: float = DEFAULT_MAX_BACKOFF) -> None:
        """
        Цикл run_once с паузой interval секунд. Упавший цикл логируется и не подтверждается —
        повтор на следующей итерации. Корректно завершается по SIGTERM/SIGINT.

        Повтор после падения — с экспоненциальной паузой (BACKOFF_FACTOR, потолок max_backoff),
        которая сбрасывается до interval после успешного цикла. Ошибки прав/адреса
        (PERMANENT_HTTP_CODES) уводят паузу на потолок сразу: повтор их не исправит.
        При interval=0 (тесты, прогон без пауз) backoff не применяется.

        max_iterations ограничивает число итераций (0 — бесконечно). Итерацией считается каждый
        вызов run_once, включая упавший на коннекте: ретрай подключения к недоступной 1С — это
        и есть итерация (внутри run_once своих ретраев нет), поэтому max_iterations ограничивает
        и число попыток подключения. Полезно для отладки/тестов.

        Таймаут запросов с interval не связан: пакет изменений обрабатывается сколько нужно,
        а не «не дольше периода опроса». Если он не задан в конструкторе, ридеры подставляют
        DEFAULT_REQUEST_TIMEOUT (см. metadata_reader).

        После каждого цикла фоном (пул потоков) запускаются полные выгрузки помеченных объектов —
        диспетчеризация только здесь (одиночный run_once лишь взводит флаги).
        """
        stop = _StopSignal()
        logger.info("Starting replication loop (interval=%ss, max_iterations=%s, timeout=%ss)",
                    interval, max_iterations, self._request_timeout)
        iterations = 0
        delay = interval
        with ThreadPoolExecutor(max_workers=self._full_load_workers,
                                thread_name_prefix='full_load') as executor:
            while not stop.requested:
                try:
                    self.run_once()
                    self._dispatch_full_loads(executor)
                    delay = interval
                except Exception as exc:
                    if interval <= 0:
                        _log_failure(exc, "Replication cycle failed, will retry")
                    elif _is_permanent_error(exc):
                        delay = max_backoff
                        _log_failure(
                            exc, "Replication cycle failed with a permanent error (check "
                            "credentials, rights and exchange plan settings), retry in %ss", delay)
                    else:
                        delay = min(max(delay, interval) * BACKOFF_FACTOR, max_backoff)
                        _log_failure(exc, "Replication cycle failed, retry in %ss", delay)

                iterations += 1
                if max_iterations > 0 and iterations >= max_iterations:
                    logger.info("Reached max_iterations (%s), stopping", max_iterations)
                    break

                stop.wait(delay)
            logger.info("Replication loop stopping, waiting for full loads to finish")
        logger.info("Replication loop stopped")

    def _dispatch_full_loads(self, executor: ThreadPoolExecutor) -> None:
        """
        Ставит в пул полные выгрузки объектов с full_load_is_required, кроме уже выполняющихся.
        Защита от повторного сабмита — множество _full_load_in_progress (под локом).
        """
        for object_full_name in self.metadata.list_full_load_required():
            with self._in_progress_lock:
                if object_full_name in self._full_load_in_progress:
                    continue
                self._full_load_in_progress.add(object_full_name)
            executor.submit(self._run_full_load, object_full_name)

    @_load_mode_tag(LOAD_MODE_FULL)
    def _run_full_load(self, object_full_name: str) -> None:
        """Фоновая полная выгрузка одного объекта; на успехе фиксирует mark_full_loaded.
        При ошибке флаг full_load_is_required остаётся → ретрай на следующем цикле."""
        try:
            self.full_load(object_full_name)
            self.metadata.mark_full_loaded(object_full_name)
        except Exception as exc:
            _log_failure(exc, "Background full_load of %s failed, will retry", object_full_name)
        finally:
            with self._in_progress_lock:
                self._full_load_in_progress.discard(object_full_name)


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
