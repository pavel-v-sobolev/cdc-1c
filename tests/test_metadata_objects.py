"""
Оффлайн-тесты реестра объектов metadata_objects_1c (внутри MetadataReader1C, на sqlite).
Проверяют синхронизацию с $metadata (dbmerge mark), пометку/снятие is_deleted, возврат объекта
и флаги полной выгрузки (require/list/mark). Без 1С/Postgres.
"""

import sqlite3
import uuid

from sqlalchemy import create_engine, select

from cdc_1c.metadata_reader import MetadataReader1C

sqlite3.register_adapter(uuid.UUID, str)


def _reader(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'm.db'}")
    return MetadataReader1C("http://x", engine=engine)


def _row(reader, name):
    t = reader.objects_table
    with reader.engine.connect() as c:
        return c.execute(select(t).where(t.c.object_name == name)).mappings().first()


def test_sync_marks_and_unmarks_deleted(tmp_path):
    reader = _reader(tmp_path)
    reader._sync_objects(["Catalog_A", "Document_B"])
    assert _row(reader, "Catalog_A")["is_deleted"] in (False, 0)
    assert _row(reader, "Catalog_A")["object_type"] == "Catalog"

    # Document_B пропал из метаданных → помечен удалённым.
    reader._sync_objects(["Catalog_A"])
    assert _row(reader, "Document_B")["is_deleted"] in (True, 1)


def test_reappear_requires_new_full_load(tmp_path):
    reader = _reader(tmp_path)
    reader._sync_objects(["Catalog_A", "Document_B"])
    reader.mark_full_loaded("Document_B")          # был выгружен: dt стоит, флаг снят
    assert _row(reader, "Document_B")["last_full_load_dt"] is not None

    reader._sync_objects(["Catalog_A"])            # пропал → is_deleted
    reader._sync_objects(["Catalog_A", "Document_B"])  # вернулся

    row = _row(reader, "Document_B")
    assert row["is_deleted"] in (False, 0)          # пометка снята (dbmerge)
    assert row["last_full_load_dt"] is None          # сброшен → считается «не выгружался»
    assert row["full_load_is_required"] in (True, 1) # нужен новый full_load


def test_require_full_load_if_new(tmp_path):
    reader = _reader(tmp_path)
    reader._sync_objects(["Catalog_A"])             # первая sync создаёт таблицу

    # Объект из пакета, которого ещё нет в реестре (пришёл раньше, чем sync его увидела) → INSERT.
    reader.require_full_load_if_new("Catalog_New")
    assert _row(reader, "Catalog_New")["full_load_is_required"] in (True, 1)

    # Существующий, ещё не выгружавшийся → флаг ставится.
    reader.require_full_load_if_new("Catalog_A")
    assert _row(reader, "Catalog_A")["full_load_is_required"] in (True, 1)

    # После успешной выгрузки повторный приход НЕ требует выгрузки заново.
    reader.mark_full_loaded("Catalog_A")
    reader.require_full_load_if_new("Catalog_A")
    row = _row(reader, "Catalog_A")
    assert row["full_load_is_required"] in (False, 0)
    assert row["last_full_load_dt"] is not None


def test_list_full_load_required_excludes_deleted_and_done(tmp_path):
    reader = _reader(tmp_path)
    reader._sync_objects(["Catalog_A", "Document_B", "Catalog_C"])
    reader.require_full_load_if_new("Catalog_A")    # требуется
    reader.require_full_load_if_new("Document_B")   # требуется, но станет удалённым
    reader.mark_full_loaded("Catalog_C")            # уже выгружен → не требуется

    reader._sync_objects(["Catalog_A", "Catalog_C"])  # Document_B удалён

    assert reader.list_full_load_required() == ["Catalog_A"]
