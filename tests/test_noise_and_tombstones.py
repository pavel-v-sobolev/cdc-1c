"""
Оффлайн-тесты записи изменений на sqlite (DBWriter1C.save, режим изменений).

Два связанных механизма:
- шумной пакет (1С переписала объект, не изменив реквизитов) не должен трогать строку: отличие
  только в exchange_message_no / Version изменением не считается, merged_on остаётся прежним,
  иначе материализатор пересчитывал бы группу впустую;
- строка, выпавшая из набора регистра/табличной части, не удаляется, а помечается: числовые
  ресурсы гасятся в NULL, merged_on поднимается — событие удаления не должно исчезать бесследно.
"""

import time

from sqlalchemy import create_engine, select, Table, MetaData

from cdc_1c import DataObject1C, NameMapper1C
from cdc_1c.db_writer import DBWriter1C
from cdc_1c.metadata_reader import MetadataObject1C

REF = "R1"


def _writer():
    return DBWriter1C(create_engine("sqlite://"), NameMapper1C())


def _rows(writer, table_name):
    tbl = Table(table_name, MetaData(), autoload_with=writer.engine)
    with writer.engine.connect() as conn:
        return [dict(r) for r in conn.execute(select(tbl)).mappings()]


# --- Шум: справочник переписан без изменения реквизитов ---

_DOC_META = MetadataObject1C("Catalog_X", {"Ref_Key": "String", "Val": "String",
                                           "Version": "String"},
                             {"Ref_Key": "String"}, object_key=None)


def _doc_rec(val, emn, version):
    return {"Ref_Key": REF, "Val": val, "Version": version,
            "is_deleted_or_empty": False, "exchange_message_no": emn}


def _save_doc(writer, val, emn, version):
    return writer.save("Catalog_X", DataObject1C(_DOC_META, [_doc_rec(val, emn, version)]))


def test_noisy_packet_does_not_touch_the_row():
    w = _writer()
    _save_doc(w, "a", emn=105, version="v1")
    before = _rows(w, "Catalog_X")[0]

    # 1С переписала объект: новый пакет и новая версия данных, реквизиты те же.
    result = _save_doc(w, "a", emn=106, version="v2")

    after = _rows(w, "Catalog_X")[0]
    assert result.updated_row_count == 0
    assert after["merged_on"] == before["merged_on"]        # материализатор ничего не пересчитает
    assert after["exchange_message_no"] == 105              # шумные поля тоже не переписаны


def test_real_change_writes_noisy_fields_too():
    w = _writer()
    _save_doc(w, "a", emn=105, version="v1")

    result = _save_doc(w, "b", emn=106, version="v2")

    after = _rows(w, "Catalog_X")[0]
    assert result.updated_row_count == 1
    assert after["Val"] == "b"
    # строку обновило настоящее изменение — вместе с ней записались и шумные поля
    assert after["exchange_message_no"] == 106 and after["Version"] == "v2"


# --- Надгробия: строка выпала из набора движений регистра ---

_REG_META = MetadataObject1C(
    "AccumulationRegister_Reg",
    {"Recorder": "String", "LineNumber": "Int64", "Kolichestvo": "Double", "Comment": "String"},
    {"Recorder": "String", "LineNumber": "Int64"}, object_key=["Recorder"],
    resources=["Kolichestvo"])


def _reg_rec(line, qty):
    return {"Recorder": REF, "LineNumber": line, "Kolichestvo": qty, "Comment": "c",
            "is_deleted_or_empty": False, "exchange_message_no": 105}


def test_row_dropped_from_the_set_is_marked_with_nulled_resource():
    w = _writer()
    w.save("AccumulationRegister_Reg",
           DataObject1C(_REG_META, [_reg_rec(1, 10), _reg_rec(2, 20)]))
    before = {r["LineNumber"]: r for r in _rows(w, "AccumulationRegister_Reg")}

    # следующий пакет привёз набор без строки 2
    time.sleep(1)   # merged_on в sqlite с точностью до секунды
    w.save("AccumulationRegister_Reg", DataObject1C(_REG_META, [_reg_rec(1, 10)]))

    rows = {r["LineNumber"]: r for r in _rows(w, "AccumulationRegister_Reg")}
    assert set(rows) == {1, 2}                       # строка не исчезла — осталась надгробием
    assert rows[2]["is_deleted_or_empty"]
    assert rows[2]["Kolichestvo"] is None            # SUM не заметит её и без фильтра по флагу
    assert rows[2]["Comment"] == "c"                 # не-ресурс сохранён: видно, что это была за строка
    assert rows[2]["merged_on"] > before[2]["merged_on"]   # событие видно материализатору
    # строка 1 не менялась и осталась нетронутой
    assert rows[1]["merged_on"] == before[1]["merged_on"]


def test_row_returning_to_the_set_is_resurrected():
    w = _writer()
    w.save("AccumulationRegister_Reg",
           DataObject1C(_REG_META, [_reg_rec(1, 10), _reg_rec(2, 20)]))
    w.save("AccumulationRegister_Reg", DataObject1C(_REG_META, [_reg_rec(1, 10)]))

    # строка 2 вернулась в набор
    w.save("AccumulationRegister_Reg",
           DataObject1C(_REG_META, [_reg_rec(1, 10), _reg_rec(2, 20)]))

    rows = {r["LineNumber"]: r for r in _rows(w, "AccumulationRegister_Reg")}
    assert not rows[2]["is_deleted_or_empty"]        # флаг снят входящим значением
    assert rows[2]["Kolichestvo"] == 20              # ресурс восстановлен
