import functools
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timedelta

import requests
from dbmerge import mergeResult
from sqlalchemy import Engine, Integer, Numeric
from sqlalchemy.exc import NoSuchTableError, OperationalError

from cdc_1c.metadata_reader import MetadataReader1C
from cdc_1c.common_functions import format_duration
from cdc_1c.data_reader import (DataReader1C, IS_DELETED_OR_EMPTY_FIELD,
                                RECORDER_FIELDS, _odata_literal)
from cdc_1c.change_reader import ChangeReader1C
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.db_writer import DBWriter1C, save_order_key
from cdc_1c.db_logs import Replicator1CLog, LOAD_TYPE_CHANGES, LOAD_TYPE_FULL
from cdc_1c.full_load_keys import FullLoadKeys
from cdc_1c.handlers import (HandlerSignals, SOURCE_CHANGES, SOURCE_FULL_LOAD)
from cdc_1c.stop_signal import StopSignal, install_signal_handlers
from cdc_1c.write_tracker import WriteTracker
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

# Полная выгрузка режется на партиции по периоду (месяц), а внутри партиции страницы берутся
# через $skip. Смысл в том, что $skip дорог: 1С на каждый запрос строит выборку заново, сортирует
# и отбрасывает первые N строк, поэтому цена растёт квадратично по числу страниц. Фильтр по дате
# переводит запрос на индекс (Дата у документа, Период у регистра входят в него), и сортируется
# уже маленький кусок.
#
# Партиция глубже этого числа страниц дробится на дни: значит месяц всё равно даёт глубокий $skip.
# Порог небольшой, потому что перечитывание уже прочитанных страниц — цена дробления, и платить
# за неё много раз не хочется.
FULL_LOAD_PARTITION_MAX_PAGES = 10

# Поля, по которым выгрузка режется на периоды, если пользователь не задал своё: у документа это
# Date, у регистра — Period. Порядок важен: у регистра сведений бывают оба.
PARTITION_DATE_FIELDS = ('Date', 'Period')

# Перепроверка кандидатов на пометку (mark_missing при выгрузке за период): сколько ключей уходит
# в один $filter. Ключи объединяются через OR, и слишком длинный URL 1С не примет.
RECHECK_KEYS_PER_REQUEST = 20


def _is_permanent_error(exc: BaseException) -> bool:
    """Ошибка, которую ретрай не исправит (см. PERMANENT_HTTP_CODES)."""
    response = getattr(exc, 'response', None)
    status = getattr(response, 'status_code', None)
    return status in PERMANENT_HTTP_CODES


# Проверка параметров конструктора: ошибка в них иначе всплывает далеко от места, где её
# допустили — 404 от 1С посреди цикла, KeyError в чужом коде, а то и молча неверная работа.
# Проверяем на месте вызова и сообщением говорим, что именно передать.
_UUID_RE = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')


def _check_odata_url(odata_url) -> str:
    """URL корня OData: http(s)://host/base/odata/standard.odata (без слеша на конце — к нему
    везде дописывается /<ресурс>)."""
    if not isinstance(odata_url, str) or not odata_url.strip():
        raise ValueError("odata_url is required: URL of the 1C OData root, e.g. "
                         "http://host/base/odata/standard.odata")
    url = odata_url.strip().rstrip('/')
    if not url.startswith(('http://', 'https://')):
        raise ValueError(f"odata_url must start with http:// or https:// (got {odata_url!r})")
    if not url.endswith('/odata/standard.odata'):
        # Не ошибка: адрес публикации бывает нестандартным. Но чаще это забытый хвост пути.
        logger.warning("odata_url %s does not end with /odata/standard.odata — is this the OData "
                       "root and not the base URL?", url)
    return url


def _check_odata_auth(odata_auth):
    """(user, password) либо None. Часто передают одну строку или один элемент — с таким requests
    уходит в 1С без авторизации или падает не по делу."""
    if odata_auth is None:
        return None
    if (isinstance(odata_auth, (tuple, list)) and len(odata_auth) == 2
            and all(isinstance(part, str) for part in odata_auth)):
        return tuple(odata_auth)
    raise ValueError("odata_auth must be a (user, password) tuple of strings, or None for "
                     f"anonymous access (got {odata_auth!r})")


def _check_exchange_name(exchange_name) -> str:
    """Имя плана обмена так, как оно уходит в URL: ExchangePlan_<имя>. Префикс/точечное имя
    (ПланОбмена.Х, ExchangePlan_Х) снимаем: в URL их дописывает сам ChangeReader1C."""
    if not isinstance(exchange_name, str) or not exchange_name.strip():
        raise ValueError("exchange_name is required: name of the 1C exchange plan "
                         "(as in the configuration, e.g. ДляВитрины)")
    name = exchange_name.strip()
    for prefix in ('ExchangePlan_', 'ПланОбмена.', 'ExchangePlan.'):
        if name.startswith(prefix):
            logger.warning("exchange_name %r: dropping the %r prefix, the plain plan name is "
                           "expected", exchange_name, prefix)
            name = name[len(prefix):]
    if '/' in name or ' ' in name:
        raise ValueError(f"exchange_name must be a bare exchange plan name (got {exchange_name!r})")
    return name


def _check_queue_guid(queue_guid) -> str:
    """Ref_Key узла обмена. Пустой — допустим: чтение изменений тогда выведет в лог список узлов
    (см. ChangeReader1C._raise_no_queue_guid). Непустой обязан быть guid: имя или код узла в URL
    даст ответ 1С, по которому это не угадать."""
    if queue_guid is None:
        return ''
    if not isinstance(queue_guid, str):
        raise ValueError(f"queue_guid must be a string Ref_Key of the exchange node (got {queue_guid!r})")
    guid = queue_guid.strip().strip('{}')
    if guid and not _UUID_RE.match(guid):
        raise ValueError(f"queue_guid must be the Ref_Key (guid) of the exchange node, not its "
                         f"code or name (got {queue_guid!r}). Leave it empty to log the list of "
                         f"available nodes")
    return guid


def _check_engine(engine) -> Engine:
    """Готовый Engine, а не строка подключения: пул и опции задаёт вызывающий (см. docstring)."""
    if isinstance(engine, Engine):
        return engine
    if isinstance(engine, str):
        raise ValueError("engine must be a SQLAlchemy Engine, not a connection string: "
                         f"pass create_engine({engine!r})")
    raise ValueError(f"engine must be a SQLAlchemy Engine (got {type(engine).__name__})")


def _check_db_connection(engine: Engine) -> Engine:
    """
    Проверяет, что БД отвечает, — до того, как её тронет первый же компонент (журнал загрузок
    создаёт свою таблицу прямо в конструкторе).

    Смысл в сообщении, а не в проверке: недоступная база даёт полтораста строк трейса сквозь пул
    SQLAlchemy и psycopg2, где полезна ровно одна строка — «хост не резолвится» или «отказано в
    соединении». Для того, кто запускает контейнер, это нечитаемо. Поднимаем ту же ошибку, но с
    коротким текстом и без чужого трейса (`from None`), добавив к ней адрес БД без пароля.

    Проверка одноразовая, на старте: разрыв связи в работающем цикле — дело обычное, его ловит
    run_forever и повторяет с backoff.
    """
    try:
        with engine.connect():
            pass
    except OperationalError as exc:
        # orig — исключение драйвера: у psycopg2 в нём та самая единственная полезная строка.
        reason = str(exc.orig or exc).strip().splitlines()[0] if exc.orig else str(exc)
        url = engine.url.render_as_string(hide_password=True)
        # ConnectionError, а не OperationalError: та печатает себя вместе с SQL-контекстом,
        # которого здесь нет, — соединение не открылось вовсе.
        raise ConnectionError(f"cannot connect to the database {url}: {reason}") from None
    return engine


def _check_db_schema(db_schema):
    """Имя схемы БД либо None (схема по умолчанию). Пустая строка — почти наверняка незаполненная
    переменная окружения, а не осознанный выбор."""
    if db_schema is None:
        return None
    if not isinstance(db_schema, str):
        raise ValueError(f"db_schema must be a schema name string or None (got {db_schema!r})")
    return db_schema.strip() or None


def _check_full_load_workers(full_load_workers) -> int:
    """Число потоков полной выгрузки: >= 1 (0 остановил бы выгрузку молча)."""
    if isinstance(full_load_workers, bool) or not isinstance(full_load_workers, int):
        raise ValueError(f"full_load_workers must be an int >= 1 (got {full_load_workers!r})")
    if full_load_workers < 1:
        raise ValueError(f"full_load_workers must be >= 1 (got {full_load_workers})")
    return full_load_workers


def _check_request_timeout(request_timeout):
    """Таймаут requests: число секунд либо (connect, read). None — значение по умолчанию
    (DEFAULT_REQUEST_TIMEOUT). Явный 0/None внутри кортежа — вечное ожидание, это не таймаут."""
    if request_timeout is None:
        return None
    values = request_timeout if isinstance(request_timeout, (tuple, list)) else (request_timeout,)
    if (len(values) not in (1, 2)
            or not all(isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0
                       for v in values)):
        raise ValueError("request_timeout must be a positive number of seconds or a "
                         f"(connect, read) pair of them (got {request_timeout!r})")
    return tuple(values) if isinstance(request_timeout, (tuple, list)) else request_timeout


def _rows_modified(result) -> int:
    """Сколько строк merge реально изменил. None — save вышел рано (пустой набор, нет метаданных)."""
    if result is None:
        return 0
    return result.inserted_row_count + result.updated_row_count + result.deleted_row_count


def _marked_result(marked: int) -> mergeResult:
    """Результат шага пометки пропавших строк в терминах mergeResult: журнал и сигнал обработчикам
    принимают именно его, а пометка — это ровно то, что dbmerge считает deleted_row_count."""
    return mergeResult(total_row_count=marked, inserted_row_count=0, updated_row_count=0,
                       deleted_row_count=marked, total_time=0.0, temp_insert_time=0.0,
                       insert_time=0.0, update_time=0.0, delete_time=0.0)


def _log_failure(exc: BaseException, message: str, *args) -> None:
    """
    Пишет в лог падение цикла. Для ошибок обмена traceback не нужен: он целиком состоит из
    внутренностей requests и ничего не добавляет к описанию, которое пришло от 1С.

    - HTTPError: описание уже выведено raise_for_status строкой выше, не дублируем;
    - прочие ошибки requests (таймаут, обрыв): traceback не нужен, но текст выводим — его нигде нет;
    - OperationalError от БД (упала, перезапустилась, кончились соединения): то же самое, полезна
      строка драйвера, а не сто строк внутренностей SQLAlchemy;
    - остальное: это уже похоже на ошибку в коде, traceback оставляем.
    """
    if isinstance(exc, requests.HTTPError):
        logger.error(message, *args)
    elif isinstance(exc, requests.RequestException):
        logger.error(f'{message}: %s', *args, exc)
    elif isinstance(exc, OperationalError):
        logger.error(f'{message}: %s', *args, str(exc.orig or exc).strip().splitlines()[0])
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


def _and_filters(*parts: str | None) -> str | None:
    """Склейка OData-фрагментов $filter по AND; None и пустые пропускаются."""
    clauses = [part for part in parts if part]
    if not clauses:
        return None
    return " and ".join(clauses)


def _month_start(moment: datetime) -> datetime:
    return moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(moment: datetime) -> datetime:
    return (moment.replace(day=28) + timedelta(days=4)).replace(day=1)


class _Partition:
    """
    Отрезок периода [start, end) полной выгрузки: за него отвечает один $filter, внутри страницы
    берутся обычным способом.

    end=None у самой свежей партиции — правая граница открыта намеренно: документы, созданные уже
    во время прогона, попадают в неё, а не остаются за краем выгрузки. Идём мы от свежих к старым,
    поэтому она же читается первой: свежие данные обычно нужнее, и при обрыве прогона они уже в БД.
    """

    def __init__(self, date_field: str, start: datetime, end: datetime | None, *, final: bool):
        self.date_field = date_field
        self.start = start
        self.end = end
        # final — партицию дробить больше некуда (день); только для неё лимит страниц не ставится.
        self.final = final

    @property
    def filter(self) -> str:
        clauses = [f"{self.date_field} ge {Replicator1C._odata_datetime(self.start)}"]
        if self.end is not None:
            clauses.append(f"{self.date_field} lt {Replicator1C._odata_datetime(self.end)}")
        return " and ".join(clauses)

    @property
    def title(self) -> str:
        return f"{self.start:%Y-%m}" if self.end is None or self.final is False else f"{self.start:%Y-%m-%d}"

    def split(self) -> list["_Partition"]:
        """Дробит месяц на дни — от свежих к старым, как и сами партиции."""
        end = self.end or _next_month(self.start)
        days = []
        day = self.start
        while day < end:
            days.append(_Partition(self.date_field, day, day + timedelta(days=1), final=True))
            day += timedelta(days=1)
        # Правая граница последнего дня остаётся открытой, если открыта у самой партиции.
        if self.end is None and days:
            days[-1] = _Partition(self.date_field, days[-1].start, None, final=True)
        return list(reversed(days))


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

    Принимает отдельные аргументы, а не объект настроек: параметры присваиваются явно и на месте
    вызова видно, что именно передано — одинаково и в python-приложении с литералами, и в
    контейнере, где значения берутся из окружения. БД передаётся готовым engine: пользователь сам
    управляет пулом и опциями, а тот же engine прокидывается в DBWriter1C.
    """

    def __init__(self, odata_url: str, odata_auth: tuple[str, str] | None,
                 exchange_name: str, queue_guid: str,
                 engine: Engine, db_schema: str | None = None,
                 db_temp_schema: str | None = None,
                 request_timeout: float | None = None,
                 full_load_workers: int = 2):
        # Включаем вывод логов, если приложение не настроило логирование само.
        _ensure_handler()
        # Перехват SIGTERM/SIGINT ставим здесь, а не при запуске цикла: run_forever типовая точка
        # входа отправляет в пул потоков, а из рабочего потока перехват поставить нельзя (см.
        # stop_signal). Конструктор же зовётся из главного.
        install_signal_handlers(quiet=False)

        # Параметры проверяем здесь, а не по месту использования: неверный адрес или не тот guid
        # иначе оборачиваются ошибкой 1С посреди первого цикла, где уже не видно, что не так.
        self.engine = _check_db_connection(_check_engine(engine))
        self.db_schema = _check_db_schema(db_schema)
        # Схема промежуточных таблиц dbmerge. Не задана — та же, что у данных. Отдельная схема
        # (например cdc_1c_tmp) держит их в стороне от таблиц с данными: в ней по определению нет
        # ничего ценного, поэтому таблицу, оставшуюся после падения процесса, там видно и не жалко.
        self.db_temp_schema = _check_db_schema(db_temp_schema)
        self._odata_url = _check_odata_url(odata_url)
        self._exchange_name = _check_exchange_name(exchange_name)
        self._queue_guid = _check_queue_guid(queue_guid)
        # odata_auth — кортеж (user, password) либо None, как в ридерах (передаётся им как есть).
        self._odata_auth = _check_odata_auth(odata_auth)
        # None → таймаут не задан явно: ридеры подставят DEFAULT_REQUEST_TIMEOUT (metadata_reader).
        self._request_timeout = _check_request_timeout(request_timeout)

        # Компоненты строятся сразу, но в сеть не ходят: MetadataReader1C создаётся пустым,
        # метаданные подгрузятся лениво при первом run_once. MetadataReader1C получает engine —
        # он же ведёт реестр объектов metadata_objects_1c (состояние полной выгрузки).
        self.metadata = MetadataReader1C(self._odata_url, odata_auth=self._odata_auth,
                                         request_timeout=self._request_timeout,
                                         engine=self.engine, schema=self.db_schema,
                                         temp_schema=self.db_temp_schema)
        self.name_mapper = NameMapper1C()

        # Фоновая полная выгрузка: пул потоков и защита от повторного сабмита одного объекта.
        self._full_load_workers = _check_full_load_workers(full_load_workers)
        self._full_load_in_progress: set[str] = set()
        # Размер страницы, который 1С реально осилила по этому объекту (см. FULL_LOAD_MIN_BATCH).
        # Пишет только поток самой выгрузки, а он на объект один (_full_load_in_progress).
        self._full_load_page_size: dict[str, int] = {}
        self._in_progress_lock = threading.Lock()
        self.changes = ChangeReader1C(self._odata_url, self._exchange_name, self._queue_guid,
                                      self.metadata, odata_auth=self._odata_auth,
                                      request_timeout=self._request_timeout)
        self.writer = DBWriter1C(engine=self.engine, name_mapper=self.name_mapper,
                                 schema=self.db_schema, temp_schema=self.db_temp_schema)
        # Лог загрузки (строка на объект) пишет оркестратор: только здесь есть контекст обмена
        # (exchange_name/message_no), а writer универсален и может делать и полную перевыгрузку.
        self.replicator_log = Replicator1CLog(self.engine, self.db_schema)

        # Реестр идущих merge — в БД, а не в памяти: обработчик может считать витрину в другом
        # процессе, и границу своего окна он обязан прижимать к НАШИМ незавершённым merge.
        self.writes = WriteTracker(self.engine, self.db_schema, self._exchange_name)
        # Сигналы обработчикам идут через handlers_1c, а не через объекты: репликатору не нужны
        # ни их код, ни общий с ними процесс (см. HandlerSignals).
        self.handler_signals = HandlerSignals(self.engine, self.db_schema)
        # Действующий StopSignal текущего run_forever — через него цикл останавливают снаружи,
        # когда он крутится не в главном потоке и своего перехвата сигналов не имеет.
        self._stop_signal: "StopSignal | None" = None


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
        finished_at=NULL и не двигает границу окна обработчика. Лог здесь, а не в DBWriter1C,
        потому что только тут есть контекст обмена (exchange_name/message_no).

        Порядок сохранения — справочники → документы → регистры (save_order_key): документы ссылаются
        на справочники, регистры — на документы, поэтому родителей пишем раньше. Внутри группы
        исходный порядок пакета (сортировка стабильна).
        """
        for object_name, data_object in sorted(self.changes.items(),
                                               key=lambda kv: save_order_key(kv[0])):
            log_id = self.replicator_log.start(
                self.changes.exchange_name, object_name, self.changes.message_no, LOAD_TYPE_CHANGES)
            table_name = self._handler_key(object_name)
            with self.writes.track(table_name):
                result = self.writer.save(object_name, data_object)
            # Одно сохранение на строку лога: счётчики и завершение — одним запросом.
            self.replicator_log.write_result(log_id, result, finish=True)
            self._signal_handlers(table_name, result, SOURCE_CHANGES)

    def _handler_key(self, object_name: str) -> str:
        """
        Имя таблицы объекта в БД — под ним объект и известен обработчикам (см. Handler1C.ON).
        Подписка идёт по имени таблицы, а не по имени объекта 1С, потому что обработчик пишет SQL
        по таблицам: имя 1С он в глаза не видит, а транслит стоит у него в запросе.
        """
        return self.name_mapper.map_object_name(object_name)

    def _signal_handlers(self, table_name: str, result, source: str) -> None:
        """
        Сообщает обработчикам, что таблица изменилась — но только если merge реально что-то сделал.
        1С регистрирует изменение объекта на любую перезапись, и в пакет приезжает масса записей,
        идентичных тому, что уже лежит в БД (шумные поля при сравнении не учитываются, см.
        DBWriter1C._noisy_fields). Обработчик всё равно выбирает данные сам, и на пустом прогоне
        его SELECT вернул бы пусто — звать незачем.

        Сообщаем через БД: ставим метку update_requested_at в handlers_1c тем, у кого эта таблица есть
        в update_on. Ни объектов обработчиков, ни их кода репликатору для этого не нужно, поэтому
        они могут работать в другом процессе или контейнере.

        Появление новой колонки — отдельный случай: значения строк оно не меняет, merged_on не
        двигает, и окно обработчика её не увидит никогда. Поэтому на added_fields подписчики
        таблицы отправляются пересобирать витрину целиком.
        """
        if result is None:
            return
        if result.added_fields:
            self.handler_signals.request_full_rebuild(
                table_name, f"{table_name} gained columns {sorted(result.added_fields)}")
        if _rows_modified(result) > 0:
            self.handler_signals.signal(table_name, source)

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
                  date_to: date | datetime | str | None = None,
                  mark_missing: bool = False) -> int:
        """
        Полная постраничная выгрузка объекта 1С в целевую таблицу: по batch_size записей за запрос,
        каждая страница сразу сохраняется через writer.save(full_load_started_at=...). Идемпотентно — повторный
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

        Гонка с изменениями: момент старта прогона (по часам БД) берётся один раз и передаётся в
        save как full_load_started_at — снимок не трогает строки, переписанные уже после старта, и не
        воскрешает удалённые за это время строки групп (регистр/ТЧ). Всё, что старше прогона, снимок
        перезаписывает: полная выгрузка остаётся способом выровнять данные. См. DBWriter1C.save.

        Необязательный фильтр по периоду: date_field — имя поля даты/времени объекта (Date у
        документов, Period у регистров), date_from/date_to — границы (datetime/date/ISO-строка,
        включительно). Транслируется в OData $filter `date_field ge …[ and date_field le …]` и
        объединяется с keyset-курсором по AND. Полезно для ручной догрузки за нужный период.

        mark_missing=True — пометить строки, которых в 1С не оказалось (см. full_load_keys). Нужно
        затем, что физическое удаление объекта в обмен не приходит вовсе, и такая строка иначе
        остаётся в таблице навсегда. Ключи прогона копятся в отдельной таблице, и после успешного
        завершения строки, которых там нет, помечаются (is_deleted_or_empty), а не удаляются:
        обработчик замечает изменение только по merged_on. Если задан период, каждый кандидат перед
        пометкой перепроверяется запросом в 1С — из окна он мог не исчезнуть, а уехать (у документа
        изменилась дата, у независимого регистра — поле ключа). По умолчанию выключено: прогон
        становится дороже, а нужен он не всем.

        Возвращает число РЕАЛЬНО изменённых строк (вставлено + обновлено + удалено, по всем
        страницам и вложенным объектам). Это проверка самого CDC: если изменения доезжают исправно,
        выгрузка находит ровно то, что уже лежит в БД, и ответ должен быть 0.
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

        # Момент старта прогона по часам БД: по нему помечаются пропавшие строки в конце прогона
        # (guard'ы save берут свою отметку на каждую страницу, см. ниже).
        started_at = self.writer.db_now()
        # Таблицы, в которые пишет этот объект: он сам и его табличные части (в метаданных они
        # лежат отдельными объектами с именем владельца в префиксе). По ним считается граница
        # незавершённых merge для guard'ов страницы.
        page_tables = self._full_load_tables(object_name)

        # Поле, по которому объект можно порезать на периоды, если он окажется глубоким. Нарезка
        # имеет смысл только там, где страницы берутся через $skip: keyset-курсор в глубину не
        # уходит и в нарезке не нуждается.
        partition_field = None if use_keyset else self._partition_date_field(object_name, date_field)

        log_id = self.replicator_log.start(self._exchange_name, object_name, None, LOAD_TYPE_FULL)
        logger.info("Full load of %s started (batch_size=%s, key=%s, paging=%s, date_filter=%s, "
                    "partition_field=%s)", object_name, batch_size, key_fields,
                    'keyset' if use_keyset else 'skip', date_filter, partition_field)
        total = 0
        rows_modified = 0
        # Ключи прогона: собираются постранично, в конце по ним помечаются пропавшие строки.
        # ExitStack — чтобы одноразовая таблица гарантированно удалилась и при ошибке прогона.
        keys = self._full_load_keys(object_name) if mark_missing else None
        stack = ExitStack()
        with stack:
            if keys is not None:
                stack.enter_context(keys)
            page_args = dict(reader=reader, key_fields=key_fields, key_types=key_types,
                             use_keyset=use_keyset, batch_size=batch_size,
                             page_tables=page_tables, keys=keys, log_id=log_id)

            # Сначала читаем объект как есть — без нарезки и без лишних запросов. Мелкому объекту
            # (а таких большинство) периоды только вредят: он укладывается в пару страниц, а за
            # нарезку пришлось бы заплатить запросом на каждый месяц истории, даже пустой.
            # Лимит страниц ставим, только если резать вообще есть по чему.
            records, modified, exhausted = self._load_pages(
                object_name, extra_filter=date_filter, **page_args,
                max_pages=FULL_LOAD_PARTITION_MAX_PAGES if partition_field else None)
            total += records
            rows_modified += modified

            if not exhausted:
                logger.info("Full load of %s: deep object (> %s pages), switching to monthly "
                            "partitions by %s", object_name, FULL_LOAD_PARTITION_MAX_PAGES,
                            partition_field)
                # Объект глубокий: $skip уже уходит далеко, и дальше цена растёт квадратично —
                # 1С на каждый запрос строит выборку заново, сортирует и отбрасывает первые N
                # строк. Перечитываем его по месяцам: фильтр по периоду переводит запрос на индекс
                # (Дата у документа, Период у регистра входят в него), и сортируется маленький
                # кусок. Прочитанные страницы перезапишутся теми же значениями — выгрузка
                # идемпотентна, и это дешевле, чем гадать о размере объекта заранее ($count 1С
                # не отдаёт).
                for partition in self._period_partitions(object_name, reader, partition_field,
                                                         date_filter):
                    records, modified, exhausted = self._load_pages(
                        object_name, extra_filter=_and_filters(partition.filter, date_filter),
                        max_pages=FULL_LOAD_PARTITION_MAX_PAGES, **page_args)
                    total += records
                    rows_modified += modified
                    if exhausted:
                        continue
                    # Глубоким оказался и месяц — дробим его на дни. Заново, а не с середины:
                    # страницы упорядочены по ключу, а не по дате, и какие строки уже прочитаны,
                    # в терминах периода неизвестно.
                    logger.info("Full load of %s: %s is deep (> %s pages), splitting it into days",
                                object_name, partition.title, FULL_LOAD_PARTITION_MAX_PAGES)
                    for day in partition.split():
                        records, modified, _ = self._load_pages(
                            object_name, extra_filter=_and_filters(day.filter, date_filter),
                            max_pages=None, **page_args)
                        total += records
                        rows_modified += modified
            if keys is not None:
                # Пометка — только здесь, после последней страницы: прогон, упавший на середине,
                # объявил бы «пропавшим» весь непрочитанный хвост объекта.
                rows_modified += self._mark_missing_rows(object_name, keys, started_at, reader,
                                                         recheck=date_filter is not None,
                                                         log_id=log_id)
        self.replicator_log.write_result(log_id, finish=True)
        logger.info("Full load of %s finished (%s records, %s rows modified)",
                    object_name, total, rows_modified)
        return rows_modified


    def _load_pages(self, object_name: str, *, reader: DataReader1C, key_fields: list[str],
                    key_types: list[str], use_keyset: bool, extra_filter: str | None,
                    batch_size: int, page_tables: list[str], keys, log_id,
                    max_pages: int | None) -> tuple[int, int, bool]:
        """
        Постраничное чтение одной выборки (объект целиком либо его партиция по периоду) с записью
        каждой страницы. Возвращает (сколько записей прочитано, сколько строк изменено, дочитано ли
        до конца).

        max_pages ограничивает число страниц: превышение означает «выборка слишком глубокая», и
        вызывающий режет её на меньшие периоды (см. full_load). None — читать до конца.
        """
        after_values = None
        skip = 0
        total = 0
        rows_modified = 0
        pages = 0
        # Начинаем с размера, подобранного по этому объекту раньше, иначе — с пробной страницы.
        page_size = min(batch_size,
                        self._full_load_page_size.get(object_name, FULL_LOAD_PROBE_BATCH))
        while True:
            # Отметка берётся на КАЖДУЮ страницу, а не одна на прогон. Guard'ы снимка проверяют
            # «строку не переписали после того, как мы прочитали эти данные», и точка отсчёта у
            # них — момент чтения страницы. С одной отметкой на прогон группа, разрезанная
            # границей страниц (табличная часть одного владельца), блокировала сама себя:
            # строки, записанные предыдущей страницей, выглядели как чужое свежее изменение,
            # и остаток группы молча не вставлялся (87 строк в 1С → 84 в БД).
            #
            # Не «сейчас», а граница по реестру незавершённых merge — та же, к которой
            # прижимается окно обработчика (WriteTracker.boundary). merged_on штампуется ВНУТРИ
            # merge-транзакции, а коммитится позже: чужой merge, начавшийся до нашего чтения и
            # закоммиченный после, оставил бы merged_on левее «сейчас», guard счёл бы строку
            # старой, и снимок затёр бы свежее изменение. Брошенные строки реестра границу не
            # держат — их отсекает отметка живости (HEARTBEAT_TTL), поэтому умерший процесс
            # тормозит выгрузку максимум на TTL, а не навсегда. Свои же записи помехой не
            # становятся: строка реестра живёт до коммита, а страницы пишутся последовательно —
            # к этому моменту нашей строки в реестре уже нет.
            page_started_at = self.writes.boundary(page_tables)
            try:
                page = reader.read_object(object_name, top=page_size, key_fields=key_fields,
                                          after_values=after_values, key_types=key_types,
                                          extra_filter=extra_filter,
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
            if keys is not None:
                # Ключи страницы — до сохранения: если save упадёт, прогон не закончится и
                # пометки не будет вовсе, а лишние ключи в одноразовой таблице никому не мешают.
                keys.add(self._page_keys(object_name, reader))
            for obj_name, data_object in reader.items():
                # Много страниц/объектов пишутся в одну строку лога — счётчики суммируются в БД.
                table_name = self._handler_key(obj_name)
                with self.writes.track(table_name):
                    result = self.writer.save(obj_name, data_object,
                                              full_load_started_at=page_started_at)
                self.replicator_log.write_result(log_id, result)
                rows_modified += _rows_modified(result)
                # Флаг на каждую страницу, а не один в конце прогона: он булев, тысяча страниц
                # поднимет его один раз, зато витрина начинает наполняться после первой же
                # страницы, а не через часы, когда выгрузка закончится.
                self._signal_handlers(table_name, result, SOURCE_FULL_LOAD)
            total += page
            pages += 1
            if page < page_size:
                break
            if max_pages is not None and pages >= max_pages:
                # Дочитать можно и так, но дальше $skip уходит в глубину — пусть вызывающий
                # порежет выборку на меньшие периоды.
                return total, rows_modified, False
            if use_keyset:
                # Курсор следующей страницы — значения ключевых полей последней записи.
                data = reader[object_name].data
                after_values = [data[f][-1] for f in key_fields]
            else:
                skip += page
            page_size = self._next_page_size(object_name, page_size, page,
                                             reader.last_response_bytes, batch_size)
        return total, rows_modified, True

    def _partition_date_field(self, object_name: str, date_field: str | None) -> str | None:
        """
        Поле, по которому режем выгрузку на периоды: заданное пользователем либо угаданное по
        метаданным — Date у документа, Period у регистра. У справочника даты нет вовсе, и это
        нормально: он и не растёт так, чтобы $skip стал проблемой.

        Имя проверяем по метаданным, а не по классу объекта: набор полей 1С отдаёт в $metadata,
        и опираться на него надёжнее, чем на разбор имени.
        """
        properties = self.metadata.get(object_name) or {}
        if date_field:
            return date_field
        for candidate in PARTITION_DATE_FIELDS:
            if properties.get(candidate) == 'DateTime':
                return candidate
        return None

    def _period_partitions(self, object_name: str, reader: DataReader1C, date_field: str | None,
                           date_filter: str | None) -> list["_Partition"]:
        """
        Режет выгрузку на месяцы — от свежих к старым. Пустой список означает «партиционирования
        нет», и объект читается одной выборкой, как раньше.

        Границы берём у самой 1С: две строки, отсортированные по полю даты в обе стороны. Запрос
        дешёвый (поле даты входит в индекс), а знать их надо, чтобы не перебирать пустые годы.
        Внутри заданного пользователем диапазона режем так же — фильтр периода просто добавляется
        к пользовательскому.

        Правая граница самой свежей партиции открыта: документ, созданный уже во время прогона,
        иначе остался бы за краем.
        """
        if date_field is None:
            return []
        oldest = reader.read_date_bound(object_name, date_field, newest=False,
                                        extra_filter=date_filter)
        if oldest is None:
            return []          # объект (или заданный период) пуст — читать нечего
        newest = reader.read_date_bound(object_name, date_field, newest=True,
                                        extra_filter=date_filter) or oldest

        partitions = []
        start = _month_start(oldest)
        last_start = _month_start(newest)
        while start <= last_start:
            end = None if start == last_start else _next_month(start)
            partitions.append(_Partition(date_field, start, end, final=False))
            start = _next_month(start)
        return list(reversed(partitions))

    def _primary_key_columns(self, object_name: str) -> dict:
        """Первичный ключ объекта в терминах целевой таблицы: {колонка: тип SQLAlchemy}. По нему
        собираются ключи прогона и по нему же идёт анти-join пометки."""
        metadata_obj = self.metadata.get(object_name)
        column_types = metadata_obj.get_column_types()
        return {self.name_mapper.map_field_name(field): column_types[field]
                for field in metadata_obj.primary_key}

    def _full_load_keys(self, object_name: str) -> FullLoadKeys:
        """Одноразовая таблица ключей прогона (см. full_load_keys)."""
        return FullLoadKeys(self.engine, target_table_name=self._handler_key(object_name),
                            key_columns=self._primary_key_columns(object_name),
                            schema=self.db_temp_schema or self.db_schema)

    def _page_keys(self, object_name: str, reader: DataReader1C) -> list[dict]:
        """Ключи строк одной страницы — только самого объекта: табличные части приезжают вложенно и
        помечаются вместе с владельцем (own-or-skip группы), отдельного снимка по ним нет."""
        data_object = reader.get(object_name)
        if data_object is None or data_object.data_length == 0:
            return []
        data = data_object.data
        fields = self.metadata.get(object_name).primary_key
        columns = [(field, self.name_mapper.map_field_name(field)) for field in fields]
        return [{column: data[field][i] for field, column in columns}
                for i in range(data_object.data_length)]

    def _mark_missing_rows(self, object_name: str, keys: FullLoadKeys, started_at,
                           reader: DataReader1C, recheck: bool, log_id: int | None = None) -> int:
        """
        Помечает строки, которых прогон в 1С не увидел, и сообщает об этом обработчикам.

        recheck=True (выгрузка шла за период) — кандидаты сперва перепроверяются в 1С: из окна
        строка могла не исчезнуть, а уехать (у документа изменилась дата, у независимого регистра —
        поле, входящее в ключ). Ответ перепроверки авторитетнее снимка: он свежее.
        """
        table_name = self._handler_key(object_name)
        try:
            target = self.writer.target_table(table_name)
        except NoSuchTableError:
            # Таблицы нет: объект пуст и в 1С, и в БД (её создаёт первая же сохранённая страница).
            # Помечать нечего.
            logger.info("Full load of %s: nothing to mark, table %s does not exist yet",
                        object_name, table_name)
            return 0
        mark_field = self.name_mapper.map_field_name(IS_DELETED_OR_EMPTY_FIELD)
        if recheck:
            candidates = keys.missing_rows(target, started_at, mark_field)
            if candidates:
                alive = self._still_in_1c(object_name, candidates, reader)
                logger.info("Full load of %s: %s of %s candidates are still in 1C (moved out of "
                            "the period, not deleted)", object_name, len(alive), len(candidates))
                keys.add(alive)
        with self.writes.track(table_name):
            marked = keys.mark_missing(target, started_at, mark_field,
                                       reset_values=self._resource_reset_values(object_name, target))
        if marked:
            logger.info("Full load of %s: %s rows are gone from 1C and were marked deleted",
                        object_name, marked)
            # В журнал пометка идёт как deleted_row_count — тем же счётчиком, которым dbmerge
            # считает строки, помеченные удалёнными.
            if log_id is not None:
                self.replicator_log.write_result(log_id, _marked_result(marked))
            self.handler_signals.signal(table_name, SOURCE_FULL_LOAD)
        return marked

    def _resource_reset_values(self, object_name: str, target) -> dict:
        """Числовые ресурсы регистра гасим в NULL вместе с пометкой — ровно как при выпадении
        строки из набора (см. DBWriter1C._resource_reset_values): SUM игнорирует NULL, и итог
        остаётся верным даже в запросе, забывшем фильтр по is_deleted_or_empty."""
        metadata_obj = self.metadata.get(object_name)
        column_types = metadata_obj.get_column_types()
        values = {}
        for resource in metadata_obj.resources:
            column = self.name_mapper.map_field_name(resource)
            if column in target.c and isinstance(column_types.get(resource), (Integer, Numeric)):
                values[column] = None
        return values

    def _still_in_1c(self, object_name: str, candidates: list[dict],
                     reader: DataReader1C) -> list[dict]:
        """
        Кандидаты, которые в 1С всё-таки есть: запрашиваем их по ключу пачками
        (RECHECK_KEYS_PER_REQUEST) и возвращаем те, что пришли в ответе.

        Запрос идёт БЕЗ фильтра по периоду — в том и смысл: проверяем существование объекта, а не
        попадание в окно.
        """
        metadata_obj = self.metadata.get(object_name)
        fields = [(field, self.name_mapper.map_field_name(field), type_name)
                  for field, type_name in metadata_obj.primary_key.items()]
        alive = []
        for start in range(0, len(candidates), RECHECK_KEYS_PER_REQUEST):
            batch = candidates[start:start + RECHECK_KEYS_PER_REQUEST]
            terms = []
            for row in batch:
                conj = ' and '.join(f"{field} eq {_odata_literal(row[column], type_name)}"
                                    for field, column, type_name in fields)
                terms.append(f"({conj})" if ' and ' in conj else conj)
            reader.read_object(object_name, extra_filter=' or '.join(terms),
                               key_fields=[fields[0][0]])
            alive.extend(self._page_keys(object_name, reader))
        return alive

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

    def _full_load_tables(self, object_name: str) -> list[str]:
        """
        Имена таблиц (в терминах реестра незавершённых merge), в которые пишет полная выгрузка
        объекта: сам объект и его табличные части — 1С отдаёт их вместе с владельцем, и страница
        сохраняет их той же записью.

        Табличные части в метаданных лежат отдельными объектами, названными «владелец_ЧастьИмени»,
        поэтому и ищутся по префиксу.
        """
        prefix = object_name + '_'
        names = [name for name in self.metadata.keys()
                 if name == object_name or name.startswith(prefix)]
        return [self._handler_key(name) for name in names]

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

        Потоков получается четыре сорта, и пулы у них раздельные: этот цикл, full_load_workers
        потоков полной выгрузки, поток отметки живости незавершённых merge (его ведёт реестр, а не
        этот цикл) и по потоку на каждого обработчика. Обработчики в пул выгрузки не сабмитятся и
        занять его не могут. Общий у них только engine, поэтому дефицит возникает не в потоках, а
        в соединениях: одновременно их держат пакет изменений, страницы выгрузки, отметка живости
        и каждый обработчик, отсюда
        pool_size >= full_load_workers + 2 + число обработчиков (см. README_HANDLERS.md).
        """
        stop = StopSignal()
        self._stop_signal = stop
        logger.info("Starting replication loop (interval=%ss, max_iterations=%s, timeout=%ss)",
                    interval, max_iterations, self._request_timeout)
        # Отметку живости незавершённых merge ведёт сам реестр (WriteTracker), а не этот
        # цикл: строки появляются и в одиночном run_once, и в вызванном руками full_load, где
        # никакого цикла нет.
        self._replication_loop(stop, interval, max_iterations, max_backoff)
        logger.info("Replication loop stopped")

    def request_stop(self) -> None:
        """
        Просит идущий run_forever завершиться после текущей итерации — то же, что SIGTERM, но
        программно. Нужна репликатору, крутящемуся не в главном потоке: свой перехват сигналов ему
        поставить нельзя (см. stop_signal), поэтому останавливает его тот, кто поток завёл.
        """
        if self._stop_signal is not None:
            self._stop_signal.requested = True

    def _replication_loop(self, stop: StopSignal, interval: float, max_iterations: int,
                          max_backoff: float) -> None:
        """Тело run_forever: цикл run_once с backoff и фоновыми полными выгрузками."""
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

    @contextmanager
    def claim_full_load(self, object_full_name: str):
        """
        Занимает объект под полную выгрузку на время блока: отдаёт True, если объект свободен, и
        False, если его уже выгружает кто-то другой (фоновая выгрузка репликатора или другое
        расписание). Множество занятых — общее с _dispatch_full_loads, поэтому claim видят обе
        стороны.

        Нужен вызывающим извне цикла — прежде всего FullLoadCron: две одновременные выгрузки одного
        объекта данные не портят (у каждого снимка свой full_load_started_at, см. DBWriter1C.save),
        но дают 1С двойную работу и две параллельные строки в replicator_1c_log.

        Сам full_load намеренно не охраняется: прямой вызов «выгрузи вот это прямо сейчас» должен
        отрабатывать всегда.
        """
        with self._in_progress_lock:
            claimed = object_full_name not in self._full_load_in_progress
            if claimed:
                self._full_load_in_progress.add(object_full_name)
        try:
            yield claimed
        finally:
            if claimed:
                with self._in_progress_lock:
                    self._full_load_in_progress.discard(object_full_name)

    def _dispatch_full_loads(self, executor: ThreadPoolExecutor) -> None:
        """
        Ставит в пул полные выгрузки объектов с full_load_is_required, кроме уже выполняющихся.
        Защита от повторного сабмита — множество _full_load_in_progress (под локом). Claim берётся
        здесь, а не внутри задания, чтобы объект не сабмитился повторно, пока ждёт своего воркера;
        снимается он в _run_full_load (finally).
        """
        for object_full_name in self.metadata.list_full_load_required():
            with self._in_progress_lock:
                if object_full_name in self._full_load_in_progress:
                    continue
                self._full_load_in_progress.add(object_full_name)
            executor.submit(self._run_full_load, object_full_name)

    @_load_mode_tag(LOAD_MODE_FULL)
    def _run_full_load(self, object_full_name: str) -> None:
        """Фоновая полная выгрузка одного объекта; на успехе фиксирует mark_full_loaded вместе с
        метриками прогона. При ошибке флаг full_load_is_required остаётся → ретрай на следующем
        цикле, а метрики не пишутся: они описывают завершённую выгрузку."""
        started = time.monotonic()
        try:
            rows_modified = self.full_load(object_full_name)
            self.metadata.mark_full_loaded(object_full_name, rows_modified=rows_modified,
                                           minutes=round((time.monotonic() - started) / 60, 3))
        except Exception as exc:
            _log_failure(exc, "Background full_load of %s failed, will retry", object_full_name)
        finally:
            with self._in_progress_lock:
                self._full_load_in_progress.discard(object_full_name)
