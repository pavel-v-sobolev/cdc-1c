"""
Оффлайн-тесты расписания полной выгрузки (FullLoadCron). Без живой 1С: метаданные подставляются
в репликатор готовыми, full_load подменяется записывающей заглушкой. Проверяются разрешение имён
(латиница из БД ↔ имена 1С), вычисление границ периода, claim от параллельной выгрузки того же
объекта и поведение самого цикла (остановка, живучесть после ошибки).
"""

from datetime import date, datetime, timedelta

import pytest

from cdc_1c import FullLoadCron
from cdc_1c.metadata_reader import MetadataObject1C
from cdc_1c.replicator import Replicator1C
from cdc_1c.stop_signal import StopSignal
from conftest import TEST_QUEUE_GUID

OBJECT_1C = "Document_ЗаказКлиента"
TABLE = "Document_ZakazKlienta"


def _replicator(db, calls, rows_modified=0):
    """Репликатор с готовыми метаданными и заглушкой full_load, пишущей вызовы в calls."""
    rep = Replicator1C(odata_url="http://x", odata_auth=None, exchange_name="E",
                       queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)
    rep.metadata.is_loaded = True
    properties = {"Ref_Key": "Guid", "Date": "DateTime", "ДатаОтгрузки": "DateTime"}
    rep.metadata[OBJECT_1C] = MetadataObject1C(OBJECT_1C, properties, {"Ref_Key": "Guid"})

    def fake_full_load(object_name, batch_size=1000, date_field=None,
                       date_from=None, date_to=None, mark_missing=False):
        calls.append({"object_name": object_name, "batch_size": batch_size,
                      "date_field": date_field, "date_from": date_from, "date_to": date_to,
                      "mark_missing": mark_missing})
        return rows_modified

    rep.full_load = fake_full_load
    return rep


def test_resolves_table_name_to_1c_name(db):
    # Настраивают расписание по тому, что видно в БД, — латиницей; в 1С уходит имя 1С.
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="0 3 * * *")
    cron.run_once()
    assert calls[0]["object_name"] == OBJECT_1C
    assert calls[0]["date_field"] is None


def test_accepts_1c_names_as_is(db):
    # Кому привычнее имена 1С — пишет их: транслитерация не единственный вход.
    calls = []
    cron = FullLoadCron(_replicator(db, calls), OBJECT_1C, cron="0 3 * * *",
                        date_field="ДатаОтгрузки", date_to=date(2026, 6, 30))
    cron.run_once()
    assert calls[0]["object_name"] == OBJECT_1C
    assert calls[0]["date_field"] == "ДатаОтгрузки"


def test_resolves_transliterated_date_field(db):
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="0 3 * * *",
                        date_field="DataOtgruzki", date_from=date(2026, 6, 1))
    cron.run_once()
    assert calls[0]["date_field"] == "ДатаОтгрузки"


def test_unknown_names_raise(db):
    calls = []
    rep = _replicator(db, calls)
    with pytest.raises(ValueError, match="not found in 1C metadata"):
        FullLoadCron(rep, "Document_NoSuchThing", cron="0 3 * * *").run_once()
    with pytest.raises(ValueError, match="not found in Document"):
        FullLoadCron(rep, TABLE, cron="0 3 * * *", date_field="NoSuchField").run_once()
    assert calls == []


def test_sliding_window_is_computed_at_run_time(db):
    # timedelta — смещение назад от сегодняшней даты, поэтому окно едет вместе с процессом.
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="0 3 * * *",
                        date_field="Date", date_from=timedelta(days=3))
    cron.run_once()
    assert calls[0]["date_from"] == date.today() - timedelta(days=3)
    assert calls[0]["date_to"] is None


def test_fixed_bounds_are_passed_as_is(db):
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="0 3 * * *", date_field="Date",
                        date_from=date(2026, 6, 1), date_to=datetime(2026, 6, 30, 23, 59, 59))
    cron.run_once()
    assert calls[0]["date_from"] == date(2026, 6, 1)
    assert calls[0]["date_to"] == datetime(2026, 6, 30, 23, 59, 59)


def test_mark_missing_is_passed_through_and_off_by_default(db):
    # Пометка пропавших строк делает прогон дороже, поэтому включается явно.
    calls = []
    rep = _replicator(db, calls)
    FullLoadCron(rep, TABLE, cron="0 3 * * *").run_once()
    FullLoadCron(rep, TABLE, cron="0 3 * * *", mark_missing=True).run_once()
    assert [c["mark_missing"] for c in calls] == [False, True]


def test_config_errors_raise_in_constructor(db):
    rep = _replicator(db, [])
    with pytest.raises(ValueError, match="crontab"):
        FullLoadCron(rep, TABLE, cron="каждую ночь")
    with pytest.raises(ValueError, match="require date_field"):
        FullLoadCron(rep, TABLE, cron="0 3 * * *", date_from=timedelta(days=1))


def test_skips_run_while_object_is_already_loading(db):
    # Фоновая выгрузка репликатора (или второе расписание) держит объект — срабатывание
    # пропускается, а не встаёт в очередь: следующее прочитает тот же период заново.
    calls = []
    rep = _replicator(db, calls)
    cron = FullLoadCron(rep, TABLE, cron="0 3 * * *")
    with rep.claim_full_load(OBJECT_1C) as claimed:
        assert claimed
        assert cron.run_once() == 0
        assert calls == []
    # Claim снят вместе с блоком — следующий прогон отрабатывает.
    cron.run_once()
    assert len(calls) == 1


def test_run_forever_runs_on_schedule_and_stops(db, monkeypatch):
    # Ждать реальную минуту незачем: проверяем, что цикл дожидается срабатывания и выходит по
    # max_runs, а не то, как спит StopSignal.
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="* * * * *")
    monkeypatch.setattr(StopSignal, "wait", lambda self, seconds: None)
    cron.run_forever(max_runs=2)
    assert len(calls) == 2


def test_failed_run_does_not_kill_the_schedule(db, monkeypatch):
    calls = []
    rep = _replicator(db, calls)
    original = rep.full_load

    def failing_full_load(object_name, **kwargs):
        if not calls:
            calls.append({"object_name": object_name, "failed": True})
            raise RuntimeError("1C is down")
        return original(object_name, **kwargs)

    rep.full_load = failing_full_load
    cron = FullLoadCron(rep, TABLE, cron="* * * * *")
    monkeypatch.setattr(StopSignal, "wait", lambda self, seconds: None)
    cron.run_forever(max_runs=2)
    assert len(calls) == 2 and calls[0]["failed"] and calls[1]["object_name"] == OBJECT_1C


def test_request_stop_before_first_run(db, monkeypatch):
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="* * * * *")
    monkeypatch.setattr(StopSignal, "wait", lambda self, seconds: cron.request_stop())
    cron.run_forever()
    assert calls == []
