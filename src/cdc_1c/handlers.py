"""
Пользовательские обработчики событий (handlers): код, который запускается, когда в целевой БД
реально изменились данные конкретных объектов 1С.

Зачем отдельный механизм. Инкрементальная витрина (или отправка изменений во внешнюю систему)
не может сама узнать, что данные приехали: она либо опрашивает БД вхолостую, либо ждёт, пока её
запустят руками. Раннер эту информацию имеет — он и сохраняет данные, — поэтому он же и зовёт
обработчик.

Обработчик — обычный импортируемый модуль:

    # handlers/zakazy_klientov.py
    ON = ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"]
    ON_FULL_LOAD = True     # звать ли на страницах полной выгрузки (по умолчанию да)
    MIN_INTERVAL = 0        # не чаще раза в N секунд (0 — без ограничения)

    def handle(context): ...

Подключается явным импортом и явным списком — никакого сканирования каталогов и магии с путями:

    from handlers import zakazy_klientov, otpravka_v_ochered

    zakazy  = HandlerRunner(engine=engine, schema='cdc_1c', handler=zakazy_klientov)
    ochered = HandlerRunner(engine=engine, schema='cdc_1c', handler=otpravka_v_ochered)
    replicator = Replicator1C(..., engine=engine, handler_runners=[zakazy, ochered])

У каждого обработчика свой раннер и свой поток, поэтому тяжёлая витрина не задерживает остальные,
но и порядок между ними не определён: на «витрина поверх витрины считается после базовой»
полагаться нельзя. Обработчики живут где угодно, лишь бы импортировались, поэтому общий код
выносится в соседний модуль обычным `from handlers._common import ...`, а не особым соглашением.

Кроме модуля, в списке можно передать функцию с теми же атрибутами (см. декоратор `handler`) или
готовый `Handler` — раннеру нужно только имя, набор объектов и вызываемое `handle`.

В `ON` перечисляются имена ТАБЛИЦ, как они выглядят в целевой БД (транслит), а не имена объектов
1С: обработчик пишет SQL по таблицам, имя 1С он в глаза не видит. Имя 1С в `ON` не совпадёт ни с
чем — as_handler ловит это на старте и подсказывает нужное написание.

И это не только «на что реагировать», но и «что я читаю»: по тому же списку считается верхняя
граница окна (см. ниже), поэтому перечислять надо ВСЕ таблицы, из которых обработчик выбирает
данные, а не только те, ради которых он написан.

Данные обработчику не передаются — он делает свой SELECT. Ему передаётся окно времени
(`context.last_run_at`, `context.boundary`), по которому он выбирает изменившееся:

    WHERE merged_on > :last_run_at

`last_run_at` — отметка предыдущего УСПЕШНОГО запуска, `NULL` — обработчик ещё ни разу не
отрабатывал. Собрать витрину заново можно, заказав пересборку:

    UPDATE <схема>.handlers_1c SET full_rebuild_is_required = true WHERE name = '<имя обработчика>';

Тогда окно откроется с начала времён (`context.last_run_at` = None), а в `context.full_rebuild`
придёт True — на случай, если пересборка у обработчика устроена иначе, чем обычный прогон. После
успеха требование снимается и в `last_full_rebuild_dt` записывается время.

Обе границы — по часам БД (тем же now(), которым dbmerge штампует merged_on), чтобы не ловить
расхождение часов между хостами.

Про верхнюю границу. Брать её как «сейчас» нельзя: merged_on пишется внутри merge-транзакции, а
коммитится минутами позже (толстая страница полной выгрузки), поэтому строка может иметь merged_on
заведомо в прошлом и при этом быть не видна SELECT-у обработчика. Отметка бы её перешагнула, и
строка потерялась бы навсегда. Поэтому граница — минимум из «сейчас» и стартов ещё не завершённых
merge по таблицам из `ON` (см. MergeTracker): всё, что обработчику не видно, записано незавершённым
merge, а его merged_on не может быть меньше старта этого merge — значит оно гарантированно окажется
правее границы и попадёт в следующее окно.

Доставка получается at-least-once: если процесс упадёт после работы обработчика, но до записи
last_run_at, окно повторится. Обработчик обязан быть идемпотентным (для витрины это выполняется
само, для отправки во внешнюю систему — забота потребителя).
"""

import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from types import ModuleType
from typing import Callable, Iterable

from sqlalchemy import (Boolean, Column, ColumnElement, DateTime, Engine, Float, MetaData, String,
                        Table, func, insert, inspect, or_, select, text, update)

from cdc_1c.db_logs import _check_create_schema
from cdc_1c.logging_config import LOAD_MODE_HANDLER, get_logger, load_mode
from cdc_1c.name_mapper import NameMapper1C

logger = get_logger(__name__)

HANDLERS_TABLE = "handlers_1c"

# Откуда пришло изменение: пакет изменений или страница полной выгрузки.
SOURCE_CHANGES = 'changes'
SOURCE_FULL_LOAD = 'full_load'
# Первый проход после старта процесса. Грязные отметки живут в памяти и перезапуск их не переживают,
# а работа могла остаться незавершённой (упали между обработкой и записью last_run_at, или процесс
# просто стоял, пока данные грузил кто-то другой). Поэтому на старте каждый обработчик считается
# грязным один раз: если делать нечего, его SELECT по окну просто вернёт пусто.
SOURCE_STARTUP = 'startup'
# Заказана пересборка витрины (full_rebuild_is_required). Отдельный повод к запуску: флаг ставят
# руками в таблице, и никаких изменений за ним не приходит — ждать сигнала можно вечно.
SOURCE_REBUILD = 'full_rebuild'

# Пауза холостого цикла потока обработчиков. Нужна не для опроса (о новых данных сообщает событие),
# а чтобы дождаться истечения MIN_INTERVAL и отпустившей границы окна, не занимая CPU.
IDLE_POLL = 1.0

# Пауза после падения обработчика. Окно при падении не сдвигается и отметка остаётся грязной,
# поэтому без паузы сломанный обработчик повторялся бы каждый холостой цикл.
RETRY_DELAY = 60.0

# Нижняя граница окна, когда last_run_at ещё нет (первый запуск или запрошен полный пересчёт).
# Заведомо раньше любого merged_on и при этом валидная дата для всех поддерживаемых СУБД.
EPOCH = datetime(1900, 1, 1)


def db_now(engine: Engine) -> datetime:
    """Текущее время по часам БД — тем же now(), которым dbmerge штампует merged_on. Сравнивать
    границы окна с python-часами нельзя: хосты расходятся."""
    with engine.connect() as conn:
        return conn.scalar(select(func.now()))


@dataclass(frozen=True)
class HandlerContext:
    """
    Что обработчик получает на вход. Данных здесь нет намеренно: обработчик делает свой SELECT —
    так он не зависит от того, в каком виде их прочитал раннер, и переживает перезапуск процесса
    (всё его состояние — в БД).
    """

    engine: Engine
    schema: str | None
    # Границы окна по часам БД: (last_run_at, boundary]. last_run_at=None — с начала времён.
    last_run_at: datetime | None
    boundary: datetime
    # Витрину просят собрать заново: обработчик ещё ни разу не отрабатывал либо кто-то заказал
    # пересборку (handlers_1c.full_rebuild_is_required). Окно при этом открыто с начала времён —
    # обработчику, который считает только по нему, ничего специально делать не надо. Флаг нужен
    # тем, у кого пересборка идёт иначе: например, не одним merge, а по частям.
    full_rebuild: bool
    # Таблицы, изменения которых привели к этому вызову, и откуда они пришли. Информационно:
    # выбирать данные всё равно по окну (за время ожидания могли измениться и другие объекты).
    objects: frozenset[str]
    sources: frozenset[str]
    logger: object


class Handler1C:
    """
    Базовый класс обработчика. Наследоваться не обязательно — раннеру достаточно `name`, `ON` и
    `handle` (годится и модуль, и функция), — но здесь лежит то, что иначе копируется из
    обработчика в обработчик.

        class ZakazyKlientov(Handler1C):
            # имена ТАБЛИЦ в целевой БД (транслит), а не имена объектов 1С
            ON = ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"]

            def setup(self, context):
                self.execute(context, DDL)

            def handle(self, context):
                ...

    В раннер передаётся ЭКЗЕМПЛЯР, а не класс: так обработчик можно параметризовать конструктором
    (одна и та же логика на две схемы — два экземпляра в списке). Но имя по умолчанию берётся от
    класса и служит ключом состояния в handlers_1c, поэтому у параметризованных экземпляров оно
    обязано различаться — иначе они молча поделили бы одну отметку last_run_at на двоих.

    Состояние экземпляра — только кэш в пределах процесса (флаг «setup сделан», скомпилированные
    выражения). Всё, что должно пережить перезапуск, живёт в handlers_1c.
    """

    # Таблицы (транслит, как в БД), на изменения которых реагирует обработчик. Это же список
    # того, что он ЧИТАЕТ: по нему считается верхняя граница окна, поэтому перечислять надо все
    # читаемые таблицы, а не только те, ради которых обработчик написан.
    ON: list[str] = []
    # Звать ли на страницах полной выгрузки. False — для тех, кому бэкфилл не нужен (рассылка,
    # отправка в очередь): иначе первоначальная выгрузка выстрелит по каждой странице.
    ON_FULL_LOAD: bool = True
    # Не чаще раза в N секунд. Ограничитель для тяжёлых витрин: во время полной выгрузки сигналы
    # идут постранично, и без паузы инкремент конкурировал бы с самой выгрузкой за БД.
    MIN_INTERVAL: float = 0
    # Имя (ключ в handlers_1c). None — имя класса. Переименование заводит новую строку состояния,
    # то есть первый прогон после переименования пойдёт с начала времён.
    NAME: str | None = None

    def __init__(self, name: str | None = None):
        """Имя можно задать экземпляру — это то, что делает параметризацию рабочей: два экземпляра
        одного класса обязаны иметь разные имена, иначе поделят одну отметку last_run_at.
        Свой __init__ в наследнике должен звать super().__init__(name)."""
        if name:
            self.NAME = name

    @property
    def name(self) -> str:
        return self.NAME or type(self).__name__

    def setup(self, context: HandlerContext) -> None:
        """
        Разовая подготовка: DDL вьюшек и целевых таблиц, создание индексов. Вызывается один раз за
        процесс, перед первым handle. Отдельно от handle именно поэтому: полная выгрузка сигналит
        постранично, и CREATE OR REPLACE VIEW на каждый вызов брал бы блокировки на пустом месте.
        """

    def handle(self, context: HandlerContext) -> None:
        """Полезная работа за окно (context.last_run_at, context.boundary]."""
        raise NotImplementedError(f"{self.name}.handle(context) is not implemented")

    # --- то, что иначе копируется из обработчика в обработчик ----------------------------------

    def since(self, context: HandlerContext) -> datetime:
        """Нижняя граница окна; None (первый запуск либо запрошен пересчёт) → с начала времён."""
        return context.last_run_at or EPOCH

    def changed_since(self, context: HandlerContext, *columns: ColumnElement) -> ColumnElement:
        """
        Условие «хоть одна из отметок свежее прошлого прогона»: `column > since` через OR.

        Колонок несколько, потому что объекты 1С приезжают в обмене независимо: строка витрины
        собирается JOIN-ом нескольких источников, и свежим может стать любой из них.

        ВАЖНО, если колонки приходят из РАЗНЫХ таблиц соединения: на объёме такой OR перестаёт
        пользоваться индексами — ни одну таблицу нельзя отфильтровать до джойна, и условие
        вырождается в Join Filter поверх полного соединения. Тогда вместо одного WHERE берут
        UNION ALL из той же вьюшки, по ветке на отметку: в каждой ветке остаётся предикат по одной
        таблице, и он опускается в её скан (см. example_config/handlers/ — там это разобрано с
        замерами). Этот помощник хорош, когда отметки лежат на одной таблице: BitmapOr из индексных
        сканов Postgres в таком случае строит сам.

        Верхней границы здесь намеренно нет, хотя окно ею закрывается. От потери строк защищает не
        этот WHERE, а само значение context.boundary: оно прижато к старту незавершённого merge, чьи
        строки обработчику ещё не видны. Строку правее границы, которая уже видна, обработчик
        посчитает раньше срока — и посчитает её же ещё раз в следующем окне, потому что last_run_at
        станет boundary. Это лишняя работа, а не пропуск, и взамен витрина получается свежее.

        Обработчику, которому повтор дорог (отправка во внешнюю систему, где каждое сообщение
        стоит денег), верхнюю границу стоит добавить самому: `column <= context.boundary`.
        """
        since = self.since(context)
        return or_(*[column > since for column in columns])

    def execute(self, context: HandlerContext, sql: str, **params) -> None:
        """Выполняет SQL, подставляя в `{schema}` целевую схему (уже в кавычках)."""
        with context.engine.begin() as conn:
            conn.execute(text(sql.format(schema=self.schema_prefix(context))), params)

    @staticmethod
    def schema_prefix(context: HandlerContext) -> str:
        """Имя схемы для подстановки в текстовый SQL; без схемы — public (схема по умолчанию)."""
        return f'"{context.schema}"' if context.schema else 'public'


@dataclass(frozen=True)
class Handler:
    """
    Нормализованный обработчик: то, с чем работает раннер, независимо от того, чем он был объявлен
    (экземпляр Handler1C, модуль или функция). Собирается функцией as_handler.
    """

    name: str
    on: frozenset[str]
    on_full_load: bool
    min_interval: float
    handle: Callable[[HandlerContext], None]
    setup: Callable[[HandlerContext], None] | None = None


def as_handler(obj) -> Handler:
    """
    Приводит объявленный пользователем обработчик к Handler.

    Принимается всё, у чего есть `handle` и непустой `ON`: экземпляр Handler1C, модуль
    (`from handlers import zakazy` → модуль целиком) или функция с теми же атрибутами. Раннеру
    важны только имя, набор объектов и вызываемое — обязательного базового класса нет.
    """
    if isinstance(obj, Handler):
        return obj
    if isinstance(obj, type):
        # Класс вместо экземпляра — типичная опечатка (`ZakazyKlientov` вместо `ZakazyKlientov()`).
        # Инстанцировать за пользователя нельзя: конструктор может требовать аргументов.
        raise TypeError(f"Handler {obj.__name__} is passed as a class, not as an instance: "
                        f"use {obj.__name__}() in the handlers list")

    handle = getattr(obj, 'handle', None)
    if handle is None and callable(obj):
        handle = obj
    if not callable(handle):
        raise AttributeError(f"Handler {obj!r} has no callable handle(context)")

    on = frozenset(getattr(obj, 'ON', ()) or ())
    if not on:
        raise AttributeError(f"Handler {_handler_name(obj)} has empty ON: it would never be called")
    # Подписка идёт по именам ТАБЛИЦ (транслит), а не по именам объектов 1С. Имя 1С в ON выглядит
    # правдоподобно, но не совпадёт ни с чем и обработчик просто никогда не позовут — молча.
    # Ловим по не-ASCII символам и сразу подсказываем, как это имя выглядит в БД.
    for name in sorted(on):
        if not name.isascii():
            raise ValueError(
                f"Handler {_handler_name(obj)}: ON must list TABLE names as they appear in the "
                f"database, not 1C object names. Replace {name!r} with "
                f"{NameMapper1C().map_object_name(name)!r}")

    setup = getattr(obj, 'setup', None)
    return Handler(name=_handler_name(obj), on=on,
                   on_full_load=bool(getattr(obj, 'ON_FULL_LOAD', True)),
                   min_interval=float(getattr(obj, 'MIN_INTERVAL', 0)),
                   handle=handle, setup=setup if callable(setup) else None)


def _handler_name(obj) -> str:
    """Имя обработчика: NAME → .name (Handler1C) → имя модуля/функции без пути пакета."""
    name = getattr(obj, 'NAME', None) or getattr(obj, 'name', None)
    if isinstance(name, str) and name:
        return name
    if isinstance(obj, ModuleType) or callable(obj):
        return obj.__name__.rsplit('.', 1)[-1]
    return type(obj).__name__


def build_handlers(objects: Iterable) -> list[Handler]:
    """Нормализует список обработчиков и проверяет уникальность имён: имя — ключ состояния в
    handlers_1c, и два одноимённых обработчика молча делили бы одну отметку last_run_at."""
    handlers = [as_handler(obj) for obj in objects]
    seen = set()
    for handler in handlers:
        if handler.name in seen:
            raise ValueError(f"Duplicate handler name {handler.name!r}: the name is the state key "
                             f"in {HANDLERS_TABLE}, set NAME to tell them apart")
        seen.add(handler.name)
    return handlers


class MergeTracker:
    """
    Стартовые отметки незавершённых merge — по объектам, чтобы посчитать верхнюю границу окна
    обработчика (см. модульную docstring).

    Отметки на объект, а не одна общая: долгий merge толстого справочника иначе держал бы границу
    всем обработчикам подряд, в том числе тем, кто этот справочник вообще не читает.

    Взятие отметки и расчёт границы делаются ПОД ОДНИМ ЛОКОМ вместе с запросом времени у БД, и это
    не перестраховка. Если бы merge сначала спрашивал у БД своё время, а регистрировался отдельным
    шагом, между этими шагами помещался бы расчёт границы: он бы этот merge ещё не увидел, а время
    взял бы уже более позднее — и строки merge оказались бы левее границы, но невидимыми, то есть
    потерянными. Под общим локом остаётся только два порядка, и оба безопасны: либо merge успел
    зарегистрироваться, и граница прижимается к его старту, либо граница взята раньше его времени
    старта — тогда его строки заведомо правее неё и попадут в следующее окно.
    """

    def __init__(self, db_now: Callable[[], datetime]):
        self._db_now = db_now
        self._lock = threading.Lock()
        # object_name → список стартов идущих сейчас merge (их может быть несколько: страницы
        # полной выгрузки идут своим потоком параллельно с пакетом изменений).
        self._in_flight: dict[str, list[datetime]] = {}

    def track(self, object_name: str) -> "_TrackedMerge":
        """
        Контекст-менеджер на время одного merge: берёт его стартовую отметку по часам БД и держит
        её в реестре до выхода из блока, т.е. до коммита merge.

        Возвращённую отметку (`.started_at`) можно использовать и как момент старта прогона для
        guard'ов полной выгрузки — это то же самое «время до первой записи по часам БД».
        """
        with self._lock:
            started_at = self._db_now()
            self._in_flight.setdefault(object_name, []).append(started_at)
        return _TrackedMerge(self, object_name, started_at)

    def _remove(self, object_name: str, started_at: datetime) -> None:
        with self._lock:
            starts = self._in_flight.get(object_name)
            if not starts:
                return
            starts.remove(started_at)
            if not starts:
                del self._in_flight[object_name]

    def is_empty(self) -> bool:
        """Нет ли сейчас незавершённых merge (проверка перед подменой реестра на общий)."""
        with self._lock:
            return not self._in_flight

    def boundary(self, object_names: Iterable[str]) -> datetime:
        """Верхняя граница окна: минимум из «сейчас» и стартов незавершённых merge по объектам."""
        with self._lock:
            now = self._db_now()
            starts = [s for name in object_names for s in self._in_flight.get(name, ())]
        return min([now, *starts])


class _TrackedMerge:
    """Один merge в реестре MergeTracker: снимается по выходу из блока, т.е. после коммита."""

    def __init__(self, tracker: MergeTracker, object_name: str, started_at: datetime):
        self._tracker = tracker
        self._object_name = object_name
        self.started_at = started_at

    def __enter__(self) -> "_TrackedMerge":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tracker._remove(self._object_name, self.started_at)


def _handlers_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        HANDLERS_TABLE, metadata,
        Column("name", String(255), primary_key=True),
        Column("enabled", Boolean, nullable=False),
        Column("last_run_at", DateTime, nullable=True),
        Column("last_error", String, nullable=True),
        # Заказ полной пересборки витрины — руками или автоматически (см. request_full_rebuild) —
        # и метрики последней пересборки. Названы по образцу metadata_objects_1c, где так же
        # устроена полная выгрузка объекта.
        Column("full_rebuild_is_required", Boolean, nullable=False, server_default=text('false')),
        Column("last_full_rebuild_dt", DateTime, nullable=True),
        Column("last_full_rebuild_minutes", Float, nullable=True),
        schema=schema_name,
    )


def _add_missing_columns(engine: Engine, table: Table) -> None:
    """
    Дописывает в существующую таблицу колонки, которых в ней ещё нет: create(checkfirst=True)
    заводит таблицу целиком, но давно созданную не трогает, и после обновления библиотеки строки
    состояния остались бы без новых колонок.
    """
    existing = {column['name'] for column in inspect(engine).get_columns(
        table.name, schema=table.schema)}
    missing = [column for column in table.columns if column.name not in existing]
    if not missing:
        return
    compiler = engine.dialect.ddl_compiler(engine.dialect, None)
    with engine.begin() as conn:
        for column in missing:
            conn.execute(text(f'ALTER TABLE {compiler.preparer.format_table(table)} '
                              f'ADD COLUMN {compiler.get_column_specification(column)}'))
    logger.info("Added columns to %s: %s", table.name, ', '.join(c.name for c in missing))


class HandlerRunner:
    """
    Исполнитель ОДНОГО обработчика: его состояние, его очередь и его поток.

    Один обработчик на раннер, а не список, — чтобы тяжёлая витрина не задерживала остальные:
    потоки у них независимые. Плата за это — порядок вызова между обработчиками не определён, и
    два обработчика, пишущие в одну целевую таблицу, теперь могут делать это одновременно. Если
    витрина строится поверх другой витрины, полагаться на «сначала базовая» нельзя.

    Очередь схлопывающая: сигнал — это не «запусти», а «объект стал грязным». Пока обработчик
    считает, прилетевшие страницы помечают тот же объект ещё раз, а не выстраиваются в очередь из
    тысячи вызовов; отработал — сразу забирает накопившееся. Поэтому сигналить можно на каждую
    страницу полной выгрузки: витрина начинает наполняться после первой же страницы и догоняет
    выгрузку с задержкой в один свой прогон.

    Поток отдельный (а не вызов прямо в цикле репликации) — чтобы обработчик не тормозил приём
    изменений: пакет не подтверждается в 1С до успешного save, копить отставание незачем.
    Пользовательский код при этом никогда не исполняется в потоках полной выгрузки — они только
    кладут сигнал.

    Раннер можно отдать нескольким репликаторам (несколько планов обмена — один обработчик):
    start/stop считающие, поток гаснет с выходом последнего.
    """

    def __init__(self, engine: Engine, schema: str | None, handler,
                 merge_tracker: "MergeTracker | None" = None):
        # Объявление разбирается и проверяется ДО первого обращения к БД: кривое (нет handle,
        # пустой ON, класс вместо экземпляра) должно ронять старт, не оставив за собой ни схемы,
        # ни строки состояния.
        self.handler = as_handler(handler)
        self.name = self.handler.name

        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema)
        self.schema = schema
        # Реестр незавершённых merge. Обычно его отдаёт репликатор при подключении (attach_merges),
        # и тогда он общий у всех раннеров и всех репликаторов — иначе граница окна не увидела бы
        # merge соседа. Свой создаётся только для standalone-использования (тесты, ручной прогон).
        self._merges = merge_tracker

        self.table = _handlers_table(MetaData(), self.schema_name)
        self.table.create(engine, checkfirst=True)
        _add_missing_columns(engine, self.table)
        self._register_handler()

        self._lock = threading.Lock()
        # Грязные отметки: что изменилось и откуда пришло. Пустой набор объектов = ждать нечего.
        # Сразу непустой: отметки живут в памяти и перезапуск процесса не переживают, поэтому
        # после старта обработчик обязан отработать хотя бы раз.
        self._dirty_objects: set[str] = set(self.handler.on)
        self._dirty_sources: set[str] = {SOURCE_STARTUP}
        # Разовая подготовка (setup) уже сделана.
        self._prepared = False
        # Когда обработчику снова можно бежать (MIN_INTERVAL), по монотонным часам.
        self._next_allowed_at = 0.0
        self._wakeup = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # Сколько репликаторов сейчас пользуются раннером (см. start/stop). Отдельный лок, а не
        # общий с грязными отметками: stop дожидается потока, а тому для работы нужен _lock.
        self._lifecycle_lock = threading.Lock()
        self._users = 0

    def __repr__(self) -> str:
        return f'<HandlerRunner {self.name}>'

    @property
    def merges(self) -> MergeTracker:
        """Реестр незавершённых merge. Свой создаётся лениво — только если раннер используют без
        репликатора: обычно реестр приходит от него (см. attach_merges)."""
        if self._merges is None:
            self._merges = MergeTracker(lambda: db_now(self.engine))
        return self._merges

    def attach_merges(self, merge_tracker: MergeTracker) -> None:
        """
        Присоединяет раннер к реестру merge репликатора.

        Реестр обязан быть ОДНИМ на все раннеры и все репликаторы: граница окна обработчика
        считается по незавершённым merge, и если репликатор пишет мимо этого реестра, его строки,
        ещё не закоммиченные, окажутся левее записанной отметки — потеряются молча. Поэтому чужой
        реестр не подменяется, а вызывает ошибку.
        """
        if self._merges is None or self._merges is merge_tracker:
            self._merges = merge_tracker
            return
        raise ValueError(
            f"Handler {self.name} is already attached to another MergeTracker. Replicators "
            f"sharing handlers must share one: pass the same HandlerRunner objects to all of them")

    def _register_handler(self) -> None:
        """Заводит строку состояния. Новый обработчик появляется с last_run_at=NULL, т.е. первым
        прогоном обработает всё с начала времён."""
        with self.engine.begin() as conn:
            known = conn.scalar(select(self.table.c.name).where(self.table.c.name == self.name))
            if known is None:
                conn.execute(insert(self.table).values(
                    name=self.name, enabled=True, last_run_at=None, last_error=None))
                logger.info("Registered handler %s", self.name)
        logger.info("Handler %s (on=%s, on_full_load=%s, min_interval=%ss)",
                    self.name, sorted(self.handler.on), self.handler.on_full_load,
                    self.handler.min_interval)

    # --- сигналы -----------------------------------------------------------------------------

    def signal(self, object_name: str, source: str) -> None:
        """
        Таблица изменилась в БД — пометить грязным и разбудить поток. Не подписан на неё — тихо
        пропускаем.

        Зовётся только когда merge реально что-то сделал: 1С регистрирует изменение объекта на
        любую перезапись, и в пакет приезжает масса записей, идентичных тому, что уже лежит в БД.
        Пустой прогон обработчику дал бы пустой SELECT — звать незачем.
        """
        if object_name not in self.handler.on:
            return
        if source == SOURCE_FULL_LOAD and not self.handler.on_full_load:
            return
        with self._lock:
            self._dirty_objects.add(object_name)
            self._dirty_sources.add(source)
        logger.debug("Signal %s (%s) → %s", object_name, source, self.name)
        self._wakeup.set()

    def request_full_rebuild(self, object_name: str, reason: str) -> None:
        """
        Просит собрать витрину заново: ставит full_rebuild_is_required. Не подписан на объект —
        ничего не делаем.

        Единственный автоматический повод — новая колонка в таблице объекта (added_fields из
        dbmerge). Инкремент её появления не видит в принципе: окно строится по merged_on, а
        merged_on двигается только у строк, у которых изменились ЗНАЧЕНИЯ; добавление колонки не
        меняет ни одного значения, и все уже лежащие строки остаются левее окна навсегда.
        """
        if object_name not in self.handler.on:
            return
        with self.engine.begin() as conn:
            conn.execute(update(self.table).where(self.table.c.name == self.name)
                         .values(full_rebuild_is_required=True))
        logger.info("Full rebuild requested for %s (%s)", self.name, reason)

    # --- исполнение --------------------------------------------------------------------------

    def is_running(self) -> bool:
        """Работает ли поток исполнения (в режиме одиночного run_once его нет)."""
        return self._thread is not None

    def start(self) -> None:
        """
        Заявляет о себе как о пользователе раннера: поток поднимается на первом вызове, дальше
        вызовы только увеличивают счётчик.

        Счётчик, а не флаг, потому что раннер может быть общим у нескольких репликаторов (несколько
        планов обмена — один обработчик). Каждый из них делает одно и то же на входе в run_forever
        и на выходе; «владельца», который один имеет право гасить поток, нет. Иначе завершение
        одного репликатора оборвало бы обработчика остальным, причём молча: соседи продолжают
        принимать изменения и копить грязные отметки, а разбирать их уже некому.

        Под локом здесь не арифметика счётчика, а связка «посчитать → проверить → создать»: без
        неё два одновременных start могут оба увидеть отсутствие потока и завести по своему,
        а два потока на один обработчик — это параллельные merge в одну целевую таблицу.
        """
        with self._lifecycle_lock:
            self._users += 1
            if self._thread is not None:
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name=f'handler:{self.name}',
                                            daemon=True)
            self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        """
        Снимает свою заявку. Когда уходит последний пользователь, поток получает команду на выход
        и дожидается ТЕКУЩЕГО прогона — он не обрывается на середине merge.

        Пока остаётся хоть один пользователь, поток продолжает работать: репликатор, закончившийся
        по max_iterations, не должен глушить обработчика соседу.
        """
        with self._lifecycle_lock:
            if self._users == 0:
                return
            self._users -= 1
            if self._users > 0 or self._thread is None:
                return
            self._stop.set()
            self._wakeup.set()
            self._thread.join(timeout)
            self._thread = None

    def _loop(self) -> None:
        with load_mode(LOAD_MODE_HANDLER):
            logger.info("Handler %s thread started", self.name)
            while not self._stop.is_set():
                # Событие сбрасываем ДО прогона: сигналы, пришедшие во время работы обработчика,
                # взведут его снова и не потеряются (набор грязных объектов у них общий).
                self._wakeup.clear()
                try:
                    self.run_if_pending()
                except Exception:
                    logger.exception("Handler %s dispatch failed", self.name)
                # Ждём сигнала, но с таймаутом: прогон могли отложить MIN_INTERVAL или ещё не
                # отпустившая граница окна — их наступление событием не сообщается.
                self._wakeup.wait(IDLE_POLL)
            logger.info("Handler %s thread stopped", self.name)

    def run_if_pending(self) -> None:
        """
        Прогон, если обработчик ждёт запуска. Публичный: в режиме одиночного run_once отдельного
        потока нет, и репликатор вызывает этот метод синхронно.
        """
        enabled, last_run_at, full_rebuild = self._read_state()
        if not enabled:
            # Выключен в таблице — грязные отметки не копим, иначе после включения он получит
            # окно, накопленное за всё время простоя, и посчитает его одним прогоном.
            self._take_dirty()
            return
        if full_rebuild:
            # Заказ пересборки сам ставит обработчик в очередь. Иначе он бы дожидался сигнала
            # об изменении подписанных объектов, а изменений может не быть неделями — флаг,
            # проставленный руками, так и лежал бы без дела.
            self._mark_dirty(set(self.handler.on), {SOURCE_REBUILD})
        if time.monotonic() < self._next_allowed_at:
            return
        self._run(last_run_at, full_rebuild)

    def _read_state(self) -> tuple[bool, datetime | None, bool]:
        """Состояние обработчика из БД: (включён, last_run_at, заказана ли пересборка).
        Читается на каждый проход — все три меняются снаружи, руками или чужим процессом."""
        t = self.table
        with self.engine.connect() as conn:
            row = conn.execute(select(t.c.enabled, t.c.last_run_at, t.c.full_rebuild_is_required)
                               .where(t.c.name == self.name)).first()
        if row is None:
            return False, None, False
        return bool(row.enabled), row.last_run_at, bool(row.full_rebuild_is_required)

    def _run(self, last_run_at: datetime | None, full_rebuild_requested: bool) -> None:
        # Грязные отметки забираем ПЕРЕД расчётом границы, а не после: сигнал приходит уже после
        # коммита своего merge, поэтому всё, что не попало в это окно, пришлёт сигнал позже и
        # взведёт обработчик заново. В обратном порядке такой сигнал мог бы быть съеден вместе со
        # снятыми отметками, и его строки ждали бы следующего, ничем не гарантированного изменения.
        objects, sources = self._take_dirty()
        if not objects:
            return

        started = time.monotonic()
        retry_delay = 0.0
        # Всё, что может упасть, — внутри try, включая расчёт границы. Иначе отметки, снятые выше,
        # пропадут вместе с исключением: обработчик перестанет вставать в очередь до следующего
        # изменения, а в handlers_1c не появится last_error, и со стороны БД он будет выглядеть
        # исправным. Так уже случалось на сравнении границы с last_run_at.
        # Пересборка — это «ни разу не отрабатывал» либо заказ через full_rebuild_is_required.
        # Окно в обоих случаях открывается с начала времён, поэтому обработчику, который считает
        # только по окну, флаг знать не обязательно. В БД last_run_at при этом сохраняется: если
        # пересборка упадёт, прежняя граница останется на месте.
        full_rebuild = full_rebuild_requested or last_run_at is None
        window_start = None if full_rebuild else last_run_at

        try:
            boundary = self.merges.boundary(self.handler.on)
            if window_start is not None and boundary <= window_start:
                # Окно пустое или вывернутое: по читаемым объектам идёт merge, начавшийся раньше
                # прошлого прогона. Ничего не берём — отметки возвращаем, вернёмся к ним позже.
                logger.debug("Handler %s: boundary %s is not past last_run_at %s, waiting",
                             self.name, boundary, window_start)
                self._mark_dirty(objects, sources)
                return

            context = HandlerContext(
                engine=self.engine, schema=self.schema, last_run_at=window_start,
                boundary=boundary, objects=frozenset(objects), sources=frozenset(sources),
                full_rebuild=full_rebuild,
                logger=get_logger(f'cdc_1c.handler.{self.name}'))

            # Разовая подготовка (DDL вьюшек и целевых таблиц) — до первого handle и только один
            # раз за процесс. Внутри try, чтобы упавший setup лёг в last_error и повторился, а не
            # уронил поток обработчика.
            if self.handler.setup is not None and not self._prepared:
                self.handler.setup(context)
                self._prepared = True
            self.handler.handle(context)
        except Exception:
            logger.exception("Handler %s failed, retry in %ss", self.name, RETRY_DELAY)
            self._save_error()
            # Возвращаем отметки: окно не сдвинулось (last_run_at не записан), но без грязного
            # флага повтор случился бы только при следующем изменении — а его может и не быть.
            self._mark_dirty(objects, sources)
            retry_delay = RETRY_DELAY
        else:
            # last_run_at = граница, ВЗЯТАЯ ДО вызова: всё, что смёржилось за время работы
            # обработчика, окажется правее неё и попадёт в следующее окно, а не потеряется.
            elapsed = time.monotonic() - started
            if self._save_success(boundary, last_run_at, full_rebuild_requested,
                                  full_rebuild, elapsed):
                logger.info("Handler %s finished in %.1fs (objects=%s)",
                            self.name, elapsed, sorted(objects))
            else:
                # Пока обработчик работал, его состояние успели поменять снаружи — например,
                # заказали пересборку. Записать свой результат значило бы этот заказ молча
                # отменить, поэтому оставляем чужое значение и взводим обработчик заново.
                logger.info("Handler %s finished, but its state changed meanwhile — rerunning",
                            self.name)
                self._mark_dirty(objects, sources)
        finally:
            # Пауза до следующего прогона: обычно MIN_INTERVAL, после падения — RETRY_DELAY, чтобы
            # сломанный обработчик не повторялся каждую секунду и не заваливал лог трейсбеками.
            delay = max(self.handler.min_interval, retry_delay)
            if delay > 0:
                self._next_allowed_at = time.monotonic() + delay

    def _take_dirty(self) -> tuple[set[str], set[str]]:
        """Забирает накопленные отметки, оставляя очередь пустой."""
        with self._lock:
            objects, sources = self._dirty_objects, self._dirty_sources
            self._dirty_objects, self._dirty_sources = set(), set()
        return objects, sources

    def _mark_dirty(self, objects: set[str], sources: set[str]) -> None:
        """Ставит обработчика в очередь, не затирая отметки, успевшие прилететь за это время.
        Используется и чтобы вернуть снятые отметки, когда прогон не состоялся или упал."""
        with self._lock:
            self._dirty_objects |= objects
            self._dirty_sources |= sources

    def _save_success(self, boundary: datetime, expected_last_run_at: datetime | None,
                      expected_rebuild: bool, full_rebuild: bool, elapsed: float) -> bool:
        """
        Двигает отметку обработчика — но только если его состояние не поменяли снаружи, пока он
        работал (`expected_*` — значения, с которыми он стартовал). False, если не сдвинул.

        Сравнение, а не безусловная запись: пересборку могли заказать уже в середине прогона, и
        безусловная запись сняла бы этот заказ, не оставив следа. Условие собрано на IS NULL /
        равенстве, а не на IS DISTINCT FROM — тот есть не во всех СУБД.

        Если прогон был пересборкой (full_rebuild), вместе с отметкой снимается требование и
        записываются её метрики — как mark_full_loaded делает для полной выгрузки объекта. Время в
        МИНУТАХ и дробное: пересборка витрины меряется десятками минут, секунды тут только мешают.

        Два разных признака рядом не по недосмотру: expected_rebuild — что стояло в БД на старте
        прогона (по нему проверяем, не заказали ли пересборку в середине), а full_rebuild — чем
        прогон оказался на самом деле. Первый в жизни прогон — пересборка, хотя её не заказывали.
        """
        t = self.table
        unchanged = (t.c.last_run_at.is_(None) if expected_last_run_at is None
                     else t.c.last_run_at == expected_last_run_at)
        values = {'last_run_at': boundary, 'last_error': None}
        if full_rebuild:
            values.update(full_rebuild_is_required=False, last_full_rebuild_dt=func.now(),
                          last_full_rebuild_minutes=round(elapsed / 60, 3))
        with self.engine.begin() as conn:
            result = conn.execute(
                update(t)
                .where(t.c.name == self.name, unchanged,
                       t.c.full_rebuild_is_required == expected_rebuild)
                .values(**values))
        return result.rowcount > 0

    def _save_error(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(update(self.table).where(self.table.c.name == self.name)
                         .values(last_error=traceback.format_exc()[-4000:]))
