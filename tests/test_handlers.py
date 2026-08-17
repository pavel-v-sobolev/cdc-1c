"""
Оффлайн-тесты пользовательских обработчиков (cdc_1c.handlers). Без 1С и без реальных merge:
проверяется механика — нормализация объявленных обработчиков, схлопывающая очередь, окно
(last_run_at, boundary], влияние незавершённых merge на верхнюю границу, реакция на результат merge
и на упавший обработчик.

Часы БД подменяются счётчиком: у sqlite CURRENT_TIMESTAMP имеет секундную точность, и два вызова
подряд дали бы одинаковое время — окно схлопнулось бы в пустое, а тест стал бы плавающим.
"""

import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from dbmerge import mergeResult
from sqlalchemy import StaticPool, create_engine, select

from cdc_1c import Handler1C
from cdc_1c.handlers import (SOURCE_CHANGES, SOURCE_FULL_LOAD, HandlerRunner, MergeTracker,
                             as_handler, build_handlers)
from cdc_1c.name_mapper import NameMapper1C


class Spy(Handler1C):
    """Обработчик-шпион: запоминает контексты вызовов, при желании падает."""

    ON = ["Catalog_X"]

    def __init__(self, name=None, on=None, on_full_load=True, min_interval=0, fail=False):
        super().__init__(name)
        if on is not None:
            self.ON = list(on)
        self.ON_FULL_LOAD = on_full_load
        self.MIN_INTERVAL = min_interval
        self.fail = fail
        self.calls = []
        self.setup_calls = 0

    def setup(self, context):
        self.setup_calls += 1

    def handle(self, context):
        self.calls.append(context)
        if self.fail:
            raise RuntimeError('handler failed on purpose')


def _clock(start=datetime(2026, 1, 1, 12, 0, 0)):
    """Часы БД: каждый вызов на секунду позже предыдущего."""
    state = {'now': start}

    def db_now():
        state['now'] += timedelta(seconds=1)
        return state['now']

    return db_now


def _runner(*handlers, db_now=None):
    engine = create_engine("sqlite://")
    tracker = MergeTracker(db_now or _clock())
    runner = HandlerRunner(engine=engine, schema=None, handlers=list(handlers),
                           merge_tracker=tracker)
    return runner, tracker


def _last_run_at(runner, name):
    return _state(runner, name).last_run_at


def _state(runner, name):
    """Строка состояния обработчика из handlers_1c."""
    with runner.engine.connect() as conn:
        return conn.execute(select(runner.table)
                            .where(runner.table.c.name == name)).one()


def _result(inserted=0, updated=0, deleted=0, added_fields=None):
    return mergeResult(total_row_count=0, inserted_row_count=inserted, updated_row_count=updated,
                       deleted_row_count=deleted, total_time=0.0, temp_insert_time=0.0,
                       insert_time=0.0, update_time=0.0, delete_time=0.0,
                       added_fields=added_fields or {})




def test_as_handler_accepts_instances_modules_and_functions():
    # Обязательного базового класса нет: раннеру нужны имя, ON и вызываемое handle.
    assert as_handler(Spy(name='spy')).name == 'spy'

    def send_to_queue(context):
        pass
    send_to_queue.ON = ["Catalog_X"]
    assert as_handler(send_to_queue).name == 'send_to_queue'

    from example_config.handlers import zakazy_klientov as module_handler
    assert as_handler(module_handler.ZakazyKlientov()).on == frozenset(
        ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"])


def test_as_handler_rejects_broken_declarations():
    class NoOn(Handler1C):
        def handle(self, context):
            pass

    with pytest.raises(AttributeError, match="ON"):
        as_handler(NoOn())

    # Класс вместо экземпляра — типичная опечатка, и она должна ловиться на старте.
    with pytest.raises(TypeError, match="instance"):
        as_handler(Spy)

    with pytest.raises(AttributeError, match="handle"):
        as_handler(object())

    # Имя объекта 1С вместо имени таблицы: подписка бы просто не сработала, и молча.
    with pytest.raises(ValueError, match="AccumulationRegister_ZakazyKlientov"):
        as_handler(Spy(on=["AccumulationRegister_ЗаказыКлиентов"]))


def test_handlers_are_signalled_by_table_name(monkeypatch):
    # Обработчик пишет SQL по таблицам, поэтому и подписывается на имя таблицы. Оркестратор знает
    # объект под именем 1С и переводит его сам.
    from cdc_1c.db_writer import DBWriter1C
    from cdc_1c.replicator import Replicator1C

    monkeypatch.setattr(DBWriter1C, "db_now", lambda self, clock=_clock(): clock())

    spy = Spy(on=["Catalog_Nomenklatura"])
    rep = Replicator1C(odata_url="http://x", odata_auth=None, exchange_name="E", queue_guid="Q",
                       engine=create_engine("sqlite://"))
    rep.install_handlers([spy])
    rep.handlers.run_pending()
    spy.calls.clear()

    assert rep._handler_key("Catalog_Номенклатура") == "Catalog_Nomenklatura"
    rep._signal_handlers(rep._handler_key("Catalog_Номенклатура"), _result(updated=1),
                         SOURCE_CHANGES)
    rep.handlers.run_pending()
    assert len(spy.calls) == 1
    assert spy.calls[0].objects == frozenset({"Catalog_Nomenklatura"})


def test_db_now_drops_the_time_zone():
    # PostgreSQL now() отдаёт timestamptz, драйвер — offset-aware datetime. А merged_on и
    # handlers_1c.last_run_at лежат в колонках без часового пояса и читаются offset-naive.
    # Сравнить их в Python нельзя, и HandlerRunner падал на boundary <= last_run_at с
    # «can't compare offset-naive and offset-aware datetimes».
    from cdc_1c.db_writer import DBWriter1C

    aware = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=3)))

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def scalar(self, _statement):
            return aware

    class _Engine:
        def connect(self):
            return _Conn()

    writer = DBWriter1C(engine=_Engine(), name_mapper=NameMapper1C())
    now = writer.db_now()

    assert now.tzinfo is None
    assert now == aware.replace(tzinfo=None), 'смещение отбрасываем, а не переводим в UTC'
    assert now > datetime(2026, 1, 1), 'сравнение с naive-отметкой больше не падает'


def test_failure_before_handle_keeps_the_handler_in_the_queue():
    # Падение ДО вызова handle (например, при расчёте границы) не должно съедать грязные отметки:
    # иначе обработчик перестанет вставать в очередь до следующего изменения, а в handlers_1c не
    # появится last_error — со стороны БД он будет выглядеть исправным.
    def broken_clock():
        raise RuntimeError('БД недоступна')

    spy = Spy()
    runner, _ = _runner(spy, db_now=broken_clock)

    runner.run_pending()

    assert spy.calls == []
    with runner.engine.connect() as conn:
        error = conn.execute(select(runner.table.c.last_error)
                             .where(runner.table.c.name == spy.name)).scalar()
    assert error and 'БД недоступна' in error
    assert runner._dirty.get(spy.name, (set(), set()))[0], 'отметки должны вернуться в очередь'


def test_build_handlers_rejects_duplicate_names():
    # Имя — ключ состояния в handlers_1c: два одноимённых поделили бы одну отметку last_run_at.
    with pytest.raises(ValueError, match="Duplicate"):
        build_handlers([Spy(), Spy()])
    assert len(build_handlers([Spy(), Spy(name='second')])) == 2


def test_setup_runs_once_before_the_first_handle():
    # Разовая подготовка отдельно от handle: полная выгрузка сигналит постранично, и DDL на каждый
    # вызов брал бы блокировки на пустом месте.
    spy = Spy()
    runner, _ = _runner(spy)

    runner.run_pending()
    assert spy.setup_calls == 1
    assert len(spy.calls) == 1

    runner.signal("Catalog_X", SOURCE_CHANGES)
    runner.run_pending()
    assert spy.setup_calls == 1, 'setup не повторяется'
    assert len(spy.calls) == 2


def test_first_run_starts_from_scratch_and_advances_window():
    # Новый обработчик считается грязным сразу: его состояние (грязные отметки) живёт в памяти и
    # перезапуск процесса не переживает, поэтому после старта он обязан отработать хотя бы раз.
    spy = Spy()
    runner, _ = _runner(spy)

    runner.run_pending()
    assert len(spy.calls) == 1
    assert spy.calls[0].last_run_at is None, 'первый прогон идёт с начала времён'
    first_boundary = spy.calls[0].boundary
    assert _last_run_at(runner, spy.name) == first_boundary

    # Без сигнала повторно не зовём: обработчик всё равно выбирает данные сам, и пустое окно ему
    # ничего не даст.
    runner.run_pending()
    assert len(spy.calls) == 1

    runner.signal("Catalog_X", SOURCE_CHANGES)
    runner.run_pending()
    assert len(spy.calls) == 2
    # Следующее окно начинается ровно там, где закончилось предыдущее — без дыр и без нахлёста.
    assert spy.calls[1].last_run_at == first_boundary
    assert spy.calls[1].boundary > first_boundary
    assert spy.calls[1].objects == frozenset({"Catalog_X"})


def test_changed_since_covers_all_sources():
    # Строка витрины собирается JOIN-ом нескольких источников, и свежим может стать любой из них.
    from sqlalchemy import Column, DateTime, MetaData, Table

    spy = Spy()
    runner, _ = _runner(spy)
    runner.run_pending()
    context = spy.calls[0]

    t = Table('m', MetaData(), Column('a', DateTime), Column('b', DateTime))
    sql = str(spy.changed_since(context, t.c.a, t.c.b))
    assert sql.count('>') == 2, sql
    assert ' OR ' in sql
    # Верхней границы нет намеренно: от пропуска строк защищает значение context.boundary,
    # а не WHERE.
    assert '<=' not in sql, sql


def test_signals_are_coalesced():
    # Сигнал — это «объект стал грязным», а не «запусти»: тысяча страниц полной выгрузки даёт
    # один прогон, а не тысячу.
    spy = Spy(on=["Catalog_X", "Catalog_Y"])
    runner, _ = _runner(spy)
    runner.run_pending()          # стартовый прогон
    spy.calls.clear()

    for _ in range(50):
        runner.signal("Catalog_X", SOURCE_FULL_LOAD)
    runner.signal("Catalog_Y", SOURCE_CHANGES)

    runner.run_pending()
    assert len(spy.calls) == 1
    assert spy.calls[0].objects == frozenset({"Catalog_X", "Catalog_Y"})
    assert spy.calls[0].sources == frozenset({SOURCE_FULL_LOAD, SOURCE_CHANGES})


def test_on_full_load_false_ignores_backfill():
    spy = Spy(on_full_load=False)
    runner, _ = _runner(spy)
    runner.run_pending()
    spy.calls.clear()

    runner.signal("Catalog_X", SOURCE_FULL_LOAD)
    runner.run_pending()
    assert spy.calls == [], 'страницы полной выгрузки такому обработчику не адресованы'

    runner.signal("Catalog_X", SOURCE_CHANGES)
    runner.run_pending()
    assert len(spy.calls) == 1


def test_boundary_is_pinned_by_unfinished_merge():
    # Главная гарантия: merged_on пишется внутри merge-транзакции, а коммитится позже, поэтому
    # строка может быть в прошлом и при этом невидимой. Граница окна прижимается к старту такого
    # merge, и его строки достаются следующему окну, а не теряются.
    spy = Spy()
    runner, tracker = _runner(spy)
    runner.run_pending()
    spy.calls.clear()

    runner.signal("Catalog_X", SOURCE_CHANGES)
    with tracker.track("Catalog_X") as merge:
        runner.run_pending()
        assert spy.calls[0].boundary == merge.started_at

    # Merge завершился — граница снова доходит до «сейчас».
    runner.signal("Catalog_X", SOURCE_CHANGES)
    runner.run_pending()
    assert spy.calls[1].boundary > merge.started_at


def test_unrelated_merge_does_not_hold_the_window():
    # Граница считается по объектам из ON: долгая выгрузка чужой таблицы не должна тормозить
    # обработчик, который её не читает.
    spy = Spy()
    runner, tracker = _runner(spy)
    runner.run_pending()
    spy.calls.clear()

    runner.signal("Catalog_X", SOURCE_CHANGES)
    with tracker.track("Catalog_OTHER") as other:
        runner.run_pending()
        assert spy.calls[0].boundary > other.started_at


def test_handler_waits_when_window_would_be_inverted():
    # Merge, начавшийся раньше прошлого прогона, делает окно пустым: брать нечего, но и отметку
    # терять нельзя — обработчик остаётся грязным и дождётся своего.
    spy = Spy()
    runner, tracker = _runner(spy)

    with tracker.track("Catalog_X"):
        runner.run_pending()                       # стартовый прогон: last_run_at = старт merge
        assert len(spy.calls) == 1
        runner.signal("Catalog_X", SOURCE_CHANGES)
        runner.run_pending()
        assert len(spy.calls) == 1, 'окно не сдвинулось — звать нечем'

    runner.run_pending()
    assert len(spy.calls) == 2, 'merge завершился — накопленная отметка отработала'


def test_failed_handler_keeps_window_and_records_error():
    spy = Spy(fail=True)
    runner, _ = _runner(spy)
    runner.run_pending()

    assert len(spy.calls) == 1
    assert _last_run_at(runner, spy.name) is None, 'упавший обработчик окно не двигает'
    with runner.engine.connect() as conn:
        error = conn.execute(select(runner.table.c.last_error)
                             .where(runner.table.c.name == spy.name)).scalar()
    assert 'handler failed on purpose' in error
    # Отметка возвращена — повтор произойдёт сам, без нового изменения (пауза RETRY_DELAY снята).
    runner._next_allowed_at.clear()
    runner.run_pending()
    assert len(spy.calls) == 2


def test_disabled_handler_does_not_accumulate_window():
    spy = Spy()
    runner, _ = _runner(spy)
    runner.run_pending()
    spy.calls.clear()

    with runner.engine.begin() as conn:
        conn.execute(runner.table.update().where(runner.table.c.name == spy.name)
                     .values(enabled=False))
    runner.signal("Catalog_X", SOURCE_CHANGES)
    runner.run_pending()
    assert spy.calls == []

    with runner.engine.begin() as conn:
        conn.execute(runner.table.update().where(runner.table.c.name == spy.name)
                     .values(enabled=True))
    runner.run_pending()
    assert spy.calls == [], 'включение само по себе прогон не назначает — ждём изменения'


def test_full_rebuild_request_opens_the_window_and_is_cleared_after_success():
    spy = Spy()
    runner, _ = _runner(spy)
    runner.run_pending()
    assert spy.calls[0].full_rebuild is True, 'первый прогон — это тоже сборка с нуля'
    spy.calls.clear()
    first = _state(runner, spy.name)
    assert first.last_run_at is not None
    assert first.last_full_rebuild_dt is not None, 'метрики пишутся и без явного заказа'
    assert first.last_full_rebuild_minutes is not None

    runner.request_full_rebuild("Catalog_X", "new column")
    assert _state(runner, spy.name).full_rebuild_is_required

    runner.signal("Catalog_X", SOURCE_CHANGES)
    runner.run_pending()
    assert spy.calls[0].last_run_at is None, 'пересборка = окно с начала времён'
    assert spy.calls[0].full_rebuild is True

    state = _state(runner, spy.name)
    assert not state.full_rebuild_is_required, 'после успеха требование снимается'
    assert state.last_full_rebuild_dt >= first.last_full_rebuild_dt
    assert state.last_full_rebuild_minutes is not None, 'сколько заняла пересборка, минуты'
    assert state.last_run_at is not None


def test_full_rebuild_request_alone_starts_the_handler():
    # Флаг ставят руками в таблице, и никакого сигнала об изменении за ним не приходит. Если бы
    # заказ не будил обработчик сам, он лежал бы без дела до ближайшего изменения подписанных
    # объектов — а его может не быть неделями.
    spy = Spy()
    runner, _ = _runner(spy)
    runner.run_pending()
    spy.calls.clear()

    runner.run_pending()
    assert spy.calls == [], 'без изменений и без заказа обработчик не зовут'

    with runner.engine.begin() as conn:
        conn.execute(runner.table.update()
                     .where(runner.table.c.name == spy.name)
                     .values(full_rebuild_is_required=True))

    runner.run_pending()
    assert len(spy.calls) == 1, 'один только заказ пересборки должен запустить обработчик'
    assert spy.calls[0].full_rebuild is True
    assert not _state(runner, spy.name).full_rebuild_is_required


def test_full_rebuild_requested_during_a_run_is_not_lost():
    # Заказ пересборки может прийти в середине прогона (новая колонка приехала пакетом, пока
    # обработчик считал). Записать поверх него свой результат — значит молча его отменить.
    spy = Spy()
    runner, _ = _runner(spy)
    runner.run_pending()
    spy.calls.clear()
    before = _last_run_at(runner, spy.name)
    original = runner.handlers[0]

    def handle_and_request_rebuild(context):
        original.handle(context)
        runner.request_full_rebuild("Catalog_X", "new column arrived mid-run")

    runner.handlers[0] = replace(original, handle=handle_and_request_rebuild)
    runner.signal("Catalog_X", SOURCE_CHANGES)
    runner.run_pending()

    state = _state(runner, spy.name)
    assert state.full_rebuild_is_required, 'заказ пересборки пережил успешный прогон'
    assert state.last_run_at == before, 'граница не сдвинулась — прогон будет повторён'

    runner.handlers[0] = original
    runner.run_pending()
    assert spy.calls[-1].last_run_at is None, 'и следующий прогон идёт с начала времён'


def test_run_forever_starts_and_stops_the_handler_thread(monkeypatch):
    from cdc_1c.replicator import Replicator1C

    spy = Spy()
    # Обработчики бегут своим потоком, а у sqlite in-memory каждое соединение — отдельная база:
    # без общего соединения поток не увидел бы созданных таблиц.
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    rep = Replicator1C(odata_url="http://x", odata_auth=None, exchange_name="E", queue_guid="Q",
                       engine=engine)

    def wait_for_the_handler(self, notify_changes=True, handlers=None):
        # Цикл с interval=0 иначе завершается раньше, чем поток обработчиков получит первый квант,
        # и тест стал бы гоночным. Ждём именно того, что проверяем.
        deadline = time.monotonic() + 5
        while not spy.calls and time.monotonic() < deadline:
            time.sleep(0.01)

    monkeypatch.setattr(Replicator1C, "run_once", wait_for_the_handler)

    rep.run_forever(interval=0, max_iterations=1, handlers=[spy])

    assert not rep.handlers.is_running(), 'поток обработчиков останавливается вместе с циклом'
    # Стартовый прогон состоялся: грязные отметки живут в памяти, поэтому после запуска процесса
    # каждый обработчик обязан отработать хотя бы раз.
    assert len(spy.calls) == 1


def test_replicator_signals_only_on_real_changes(monkeypatch):
    # Гейт «звать только если merge реально что-то сделал» живёт в оркестраторе: 1С регистрирует
    # изменение объекта на любую перезапись, и пустых прогонов в пакете больше, чем содержательных.
    from cdc_1c.db_writer import DBWriter1C
    from cdc_1c.replicator import Replicator1C

    # Те же поддельные часы: боевые здесь — CURRENT_TIMESTAMP sqlite с точностью до секунды,
    # и все окна теста уложились бы в одно значение.
    monkeypatch.setattr(DBWriter1C, "db_now", lambda self, clock=_clock(): clock())

    spy = Spy()
    rep = Replicator1C(odata_url="http://x", odata_auth=None, exchange_name="E", queue_guid="Q",
                       engine=create_engine("sqlite://"))
    rep.install_handlers([spy])
    rep.handlers.run_pending()
    spy.calls.clear()

    rep._signal_handlers("Catalog_X", _result(), SOURCE_CHANGES)
    rep.handlers.run_pending()
    assert spy.calls == [], 'нулевой прогон merge обработчику ничего не даёт'

    rep._signal_handlers("Catalog_X", _result(updated=1), SOURCE_CHANGES)
    rep.handlers.run_pending()
    assert len(spy.calls) == 1

    # Новая колонка значений не меняет и merged_on не двигает — инкремент её не увидит никогда,
    # поэтому подписчики объекта отправляются пересчитывать всё.
    spy.calls.clear()
    rep._signal_handlers("Catalog_X", _result(inserted=1, added_fields={'Новый': 'String'}),
                         SOURCE_CHANGES)
    rep.handlers.run_pending()
    assert spy.calls[0].last_run_at is None
