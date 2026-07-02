"""
Оффлайн-тесты version-guard полной выгрузки (DBWriter1C.save, full_load=True) на sqlite.
Изменения штампуются exchange_message_no (emn) >= 1, полная выгрузка — emn=0. Проверяем, что
устаревший снимок full_load не затирает более свежие изменения и не воскрешает удалённые строки
групп (регистр/табличная часть). Ключи строковые — тип ключа на version-guard (по emn) не влияет.
"""

from sqlalchemy import create_engine, select, Table, MetaData

from cdc_1c import DataObject1C, NameMapper1C
from cdc_1c.db_writer import DBWriter1C
from cdc_1c.metadata_reader import MetadataObject1C

REF = "R1"
REF2 = "R2"


def _writer():
    engine = create_engine("sqlite://")
    return DBWriter1C(engine, NameMapper1C())


def _rows(writer, table_name):
    tbl = Table(table_name, MetaData(), autoload_with=writer.engine)
    with writer.engine.connect() as conn:
        return [dict(r) for r in conn.execute(select(tbl)).mappings()]


# --- Документ/справочник: одиночная запись по ключу, update-guard ---

_DOC_META = MetadataObject1C("Catalog_X", {"Ref_Key": "String", "Val": "String"},
                             {"Ref_Key": "String"}, object_key=None)


def _doc(records):
    return DataObject1C(_DOC_META, records)


def _doc_rec(ref, val, emn):
    return {"Ref_Key": ref, "Val": val, "is_deleted_or_empty": False, "exchange_message_no": emn}


def test_full_load_does_not_overwrite_newer_change():
    w = _writer()
    # change записал строку (emn=105)
    w.save("Catalog_X", _doc([_doc_rec(REF, "change", 105)]), full_load=False)
    # полная выгрузка (emn=0) со своим (устаревшим) значением — guard не даёт перезаписать
    w.save("Catalog_X", _doc([_doc_rec(REF, "fullload", 0)]), full_load=True)

    rows = {r["Ref_Key"]: r for r in _rows(w, "Catalog_X")}
    assert rows[REF]["Val"] == "change"
    assert rows[REF]["exchange_message_no"] == 105


def test_change_overwrites_full_load_row():
    w = _writer()
    # сначала легла полная выгрузка (emn=0)
    w.save("Catalog_X", _doc([_doc_rec(REF, "fullload", 0)]), full_load=True)
    # затем пришло изменение (emn=106) — перезаписывает базовую строку
    w.save("Catalog_X", _doc([_doc_rec(REF, "change", 106)]), full_load=False)

    rows = {r["Ref_Key"]: r for r in _rows(w, "Catalog_X")}
    assert rows[REF]["Val"] == "change" and rows[REF]["exchange_message_no"] == 106


def test_full_load_inserts_untouched_row():
    w = _writer()
    w.save("Catalog_X", _doc([_doc_rec(REF, "change", 105)]), full_load=False)
    # строку REF2 изменения не приносили — полная выгрузка её вставляет (backfill)
    w.save("Catalog_X", _doc([_doc_rec(REF, "old", 0), _doc_rec(REF2, "backfill", 0)]), full_load=True)

    rows = {r["Ref_Key"]: r for r in _rows(w, "Catalog_X")}
    assert rows[REF]["Val"] == "change"          # существующую свежую не тронули
    assert rows[REF2]["Val"] == "backfill"       # новую — вставили


# --- Групповой объект (табличная часть): own-or-skip по группе (Ref_Key) ---

_TP_META = MetadataObject1C(
    "Document_X_Rows", {"Ref_Key": "String", "LineNumber": "Int64", "Val": "String"},
    {"Ref_Key": "String", "LineNumber": "Int64"}, object_key=["Ref_Key"])


def _tp(records):
    return DataObject1C(_TP_META, records)


def _tp_rec(ref, line, val, emn):
    return {"Ref_Key": ref, "LineNumber": line, "Val": val,
            "is_deleted_or_empty": False, "exchange_message_no": emn}


def test_full_load_does_not_resurrect_deleted_group_row():
    w = _writer()
    # change заменил набор группы REF: строки 1,2 (emn=105); строка 3 удалена
    w.save("Document_X_Rows", _tp([_tp_rec(REF, 1, "a", 105), _tp_rec(REF, 2, "b", 105)]),
           full_load=False)
    # устаревший снимок full_load (emn=0) видит и строку 3 — не должен её воскресить,
    # и не должен тронуть «горячую» группу
    w.save("Document_X_Rows", _tp([_tp_rec(REF, 1, "a", 0), _tp_rec(REF, 2, "b", 0),
                                   _tp_rec(REF, 3, "resurrected", 0)]), full_load=True)

    rows = {(r["Ref_Key"], r["LineNumber"]): r for r in _rows(w, "Document_X_Rows")}
    assert (REF, 3) not in rows                    # воскрешение заблокировано insert_condition
    assert rows[(REF, 1)]["exchange_message_no"] == 105   # горячая группа не тронута
    assert rows[(REF, 2)]["exchange_message_no"] == 105


def test_full_load_replaces_own_group():
    w = _writer()
    # группа REF2 создана только полной выгрузкой (emn=0): строки 1,2
    w.save("Document_X_Rows", _tp([_tp_rec(REF2, 1, "x", 0), _tp_rec(REF2, 2, "y", 0)]),
           full_load=True)
    # повторная выгрузка: в источнике осталась только строка 1 → строка 2 должна удалиться
    w.save("Document_X_Rows", _tp([_tp_rec(REF2, 1, "x", 0)]), full_load=True)

    rows = {(r["Ref_Key"], r["LineNumber"]): r for r in _rows(w, "Document_X_Rows")}
    assert set(rows) == {(REF2, 1)}               # «свою» группу заменили снимком (строка 2 удалена)
