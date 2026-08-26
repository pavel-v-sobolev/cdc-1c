"""
Оффлайн-тесты пометки строк, пропавших из 1С (full_load(mark_missing=True)).

Без живой 1С: чтение страниц подменяется, всё остальное — настоящее, включая запись в БД, таблицу
ключей прогона и финальный UPDATE. Проверяется то, ради чего механизм и сделан: строка, которой в
1С больше нет, помечается (а не удаляется), свежую строку пометка не трогает, а при выгрузке за
период кандидат сперва перепроверяется в 1С — из окна он мог уехать, а не исчезнуть.
"""

from datetime import date, timedelta

import pytest
from sqlalchemy import MetaData, Table, select, text

from cdc_1c import DataObject1C
from cdc_1c.data_reader import DataReader1C
from cdc_1c.metadata_reader import MetadataObject1C
from cdc_1c.replicator import Replicator1C
from conftest import TEST_QUEUE_GUID

CATALOG = "Catalog_X"
REGISTER = "InformationRegister_R"


def _replicator(db):
    rep = Replicator1C(odata_url="http://x", odata_auth=None, exchange_name="E",
                       queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)
    rep.metadata.is_loaded = True
    rep.metadata[CATALOG] = MetadataObject1C(
        CATALOG, {"Ref_Key": "String", "Val": "String"}, {"Ref_Key": "String"}, object_key=None)
    # Независимый регистр сведений: ключ составной, регистратора нет — сегодня у него нет вообще
    # никакого механизма удаления, поэтому он здесь и проверяется.
    rep.metadata[REGISTER] = MetadataObject1C(
        REGISTER, {"Period": "String", "Sklad": "String", "Kolichestvo": "Int64"},
        {"Period": "String", "Sklad": "String"}, object_key=None,
        dimensions=["Sklad"], resources=["Kolichestvo"])
    return rep


def _record(**fields):
    """Запись в том виде, в каком её отдаёт reader: со спец-полями."""
    return {**fields, "is_deleted_or_empty": False, "exchange_message_no": 0}


def _pages(rep, object_name, monkeypatch, pages, recheck_answer=None):
    """
    Подменяет чтение страниц: pages — список списков записей, по одной странице на вызов.
    recheck_answer — что 1С отвечает на перепроверку по ключу (запрос без пагинации).
    """
    meta = rep.metadata[object_name]
    remaining = list(pages)
    calls = {"pages": 0, "recheck": []}

    def fake_read_object(self, name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None):
        if top is None:
            # Перепроверка кандидатов: без $top и с фильтром по ключам.
            calls["recheck"].append(extra_filter)
            records = list(recheck_answer or [])
        else:
            calls["pages"] += 1
            records = remaining.pop(0) if remaining else []
        self.clear()
        self[name] = DataObject1C(meta, [dict(r) for r in records])
        return len(records)

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    # Границы периода эти тесты не проверяют: без них выгрузка идёт одной выборкой, как и до
    # нарезки на периоды (см. Replicator1C._period_partitions).
    monkeypatch.setattr(DataReader1C, "read_date_bound", lambda *a, **k: None)
    return calls


def _rows(db, table_name):
    tbl = Table(table_name, MetaData(), schema=db.schema, autoload_with=db.engine)
    with db.engine.connect() as conn:
        return {r["Ref_Key"] if "Ref_Key" in r else (r["Period"], r["Sklad"]): dict(r)
                for r in conn.execute(select(tbl)).mappings()}


def test_row_gone_from_1c_is_marked(db, monkeypatch):
    rep = _replicator(db)
    alive = [_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]
    _pages(rep, CATALOG, monkeypatch, [alive])
    rep.full_load(CATALOG, batch_size=10)
    before = _rows(db, CATALOG)

    # Второй прогон: "b" из 1С исчез (удалён физически — в обмен такое не приходит вовсе).
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True)

    after = _rows(db, CATALOG)
    assert after["b"]["is_deleted_or_empty"] is True
    # Помечена, а не удалена, и merged_on поднят — иначе обработчик события не увидит.
    assert after["b"]["merged_on"] > before["b"]["merged_on"]
    assert after["a"]["is_deleted_or_empty"] is False
    assert after["a"]["merged_on"] == before["a"]["merged_on"]


def test_without_flag_nothing_is_marked(db, monkeypatch):
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)

    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10)          # по умолчанию mark_missing=False

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is False


def test_row_written_during_the_run_is_not_marked(db, monkeypatch):
    # Гонка с изменениями: строку переписали уже после старта прогона, в снимок она не попала.
    # Guard по merged_on тот же, что и у самой выгрузки: такую строку снимок не трогает.
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)
    with db.engine.begin() as conn:
        conn.execute(text(f'UPDATE "{db.schema}"."{CATALOG}" '
                          f"SET merged_on = now() + interval '1 hour' WHERE \"Ref_Key\" = 'b'"))

    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True)

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is False


def test_candidate_still_in_1c_survives_recheck(db, monkeypatch):
    # Выгрузка за период: строки может не быть в окне, потому что у неё изменилась дата, а не
    # потому что её удалили. Перед пометкой кандидат перепроверяется запросом по ключу.
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)

    calls = _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]],
                   recheck_answer=[_record(Ref_Key="b", Val="2")])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True,
                  date_field="Date", date_from=date(2026, 6, 1))

    assert calls["recheck"], "перепроверка не выполнялась"
    assert "Ref_Key eq 'b'" in calls["recheck"][0]
    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is False


def test_candidate_absent_in_1c_is_marked_after_recheck(db, monkeypatch):
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)

    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]], recheck_answer=[])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True,
                  date_field="Date", date_from=date(2026, 6, 1))

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is True


def test_independent_register_row_is_marked_and_resource_reset(db, monkeypatch):
    rep = _replicator(db)
    both = [_record(Period="2026-06-01", Sklad="s1", Kolichestvo=5),
            _record(Period="2026-06-01", Sklad="s2", Kolichestvo=7)]
    _pages(rep, REGISTER, monkeypatch, [both])
    rep.full_load(REGISTER, batch_size=10)

    _pages(rep, REGISTER, monkeypatch, [[both[0]]])
    rep.full_load(REGISTER, batch_size=10, mark_missing=True)

    gone = _rows(db, REGISTER)[("2026-06-01", "s2")]
    assert gone["is_deleted_or_empty"] is True
    # Ресурс гасится в NULL: SUM игнорирует NULL, и итог остаётся верным даже в запросе,
    # забывшем фильтр по is_deleted_or_empty.
    assert gone["Kolichestvo"] is None


def test_keys_table_is_dropped_after_the_run(db, monkeypatch):
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True)

    with db.engine.connect() as conn:
        left = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = :s "
                                 "AND tablename LIKE 'tmpkeys%'"), {"s": db.schema}).all()
    assert left == []


def test_empty_object_without_table_is_not_a_failure(db, monkeypatch):
    # Объект пуст и в 1С, и в БД: таблицу создаёт первая сохранённая страница, а её не было.
    # Помечать нечего — прогон обязан пройти спокойно.
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[]])

    assert rep.full_load(CATALOG, batch_size=10, mark_missing=True) == 0
