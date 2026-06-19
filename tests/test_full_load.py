"""
Оффлайн-тест постраничной полной выгрузки Replicator1C.full_load. Без живой 1С: чтение страниц
подменяется (read_object), проверяется логика цикла — keyset-пагинация по Ref_Key (курсор after_key),
условие останова, upsert без удаления (delete=False) для каждого объекта страницы и одна строка в
replicator_1c_log.
"""

import uuid

import pytest
from sqlalchemy import create_engine, select

from cdc_1c import DataObject1C
from cdc_1c.data_reader import DataReader1C
from cdc_1c.metadata_reader import MetadataObject1C, MetadataReader1C
from cdc_1c.replicator import Replicator1C


def _replicator():
    engine = create_engine("sqlite://")  # in-memory
    rep = Replicator1C(odata_url="http://x", odata_auth=None,
                       exchange_name="E", queue_guid="Q", engine=engine)
    # Метаданные «уже загружены» — full_load не пойдёт в сеть; primary_key даёт $orderby.
    rep.metadata.is_loaded = True
    rep.metadata["Catalog_X"] = MetadataObject1C({"Ref_Key": "Guid"}, {"Ref_Key": "Guid"},
                                                 object_key=None)
    return rep


def test_full_load_paging(monkeypatch):
    rep = _replicator()
    meta = rep.metadata["Catalog_X"]
    pages = iter([2, 2, 1])      # batch_size=2: две полные страницы и хвост → останов
    calls = []
    page_last_key = []           # последний Ref_Key каждой выданной страницы (= ожидаемый курсор)
    counter = {"v": 0}

    def fake_read_object(self, object_name, top=None, key_field="Ref_Key", after_key=None,
                         key_is_guid=True):
        calls.append({"top": top, "key_field": key_field, "after_key": after_key,
                      "key_is_guid": key_is_guid})
        n = next(pages)
        keys = []
        for _ in range(n):       # детерминированно возрастающие Ref_Key
            counter["v"] += 1
            keys.append(uuid.UUID(int=counter["v"]))
        self.clear()
        self[object_name] = DataObject1C(meta, [{"Ref_Key": k} for k in keys])
        page_last_key.append(keys[-1] if keys else None)
        return n

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)

    saved = []
    rep.writer.save = lambda name, obj, delete=True: saved.append((name, obj.data_length, delete))

    rep.full_load("Catalog_X", batch_size=2)

    # keyset: первый запрос без курсора, далее after_key = последний guid предыдущей страницы;
    # лимит top=batch_size, ключ Ref_Key (guid); останов на неполной странице (3 запроса).
    assert [c["after_key"] for c in calls] == [None, page_last_key[0], page_last_key[1]]
    assert all(c["top"] == 2 for c in calls)
    assert all(c["key_field"] == "Ref_Key" and c["key_is_guid"] is True for c in calls)
    # каждая страница сохранена upsert-ом без удаления; всего 2+2+1 записей.
    assert [s[2] for s in saved] == [False, False, False]
    assert sum(s[1] for s in saved) == 5

    # одна строка лога: это не пакет обмена (message_no=NULL), загрузка завершена (finished_at set).
    log = rep.replicator_log.table
    with rep.engine.connect() as conn:
        rows = conn.execute(select(log.c.object, log.c.message_no, log.c.finished_at)).all()
    assert len(rows) == 1
    assert rows[0].object == "Catalog_X" and rows[0].message_no is None
    assert rows[0].finished_at is not None


def test_full_load_register_paging(monkeypatch):
    # Регистр: keyset по Recorder (строковый литерал), курсор — Recorder последней entry страницы.
    rep = _replicator()
    rep.metadata["AccumulationRegister_R"] = MetadataObject1C(
        {"Recorder": "Guid"}, {"Recorder": "Guid", "LineNumber": "Int64", "Recorder_Type": "String"},
        object_key=["Recorder", "Recorder_Type"])
    meta = rep.metadata["AccumulationRegister_R"]
    pages = iter([2, 1])
    calls = []
    page_last_key = []
    counter = {"v": 0}

    def fake_read_object(self, object_name, top=None, key_field="Ref_Key", after_key=None,
                         key_is_guid=True):
        calls.append({"key_field": key_field, "after_key": after_key, "key_is_guid": key_is_guid})
        n = next(pages)
        recs = []
        for _ in range(n):
            counter["v"] += 1
            recs.append(uuid.UUID(int=counter["v"]))
        self.clear()
        # две строки движений на регистратора — курсор всё равно берётся по последней записи.
        self[object_name] = DataObject1C(meta, [{"Recorder": r, "LineNumber": ln}
                                                for r in recs for ln in (1, 2)])
        page_last_key.append(recs[-1] if recs else None)
        return n

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, delete=True: None

    rep.full_load("AccumulationRegister_R", batch_size=2)

    assert all(c["key_field"] == "Recorder" and c["key_is_guid"] is False for c in calls)
    assert [c["after_key"] for c in calls] == [None, page_last_key[0]]


def test_full_load_empty_object(monkeypatch):
    rep = _replicator()

    def fake_read_object(self, object_name, top=None, key_field="Ref_Key", after_key=None,
                         key_is_guid=True):
        self.clear()
        return 0     # объект пуст: первая же страница неполная → один запрос и останов

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    saved = []
    rep.writer.save = lambda name, obj, delete=True: saved.append(name)

    rep.full_load("Catalog_X", batch_size=2)
    assert saved == []   # сохранять нечего, но лог-строка с finished_at должна появиться
    with rep.engine.connect() as conn:
        rows = conn.execute(select(rep.replicator_log.table.c.finished_at)).all()
    assert len(rows) == 1 and rows[0].finished_at is not None


def test_full_load_rejects_keyless_object():
    # Независимый регистр сведений: ключ без Ref_Key и Recorder → курсор keyset не выбрать.
    rep = _replicator()
    rep.metadata["InformationRegister_Indep"] = MetadataObject1C(
        {"Period": "DateTime"}, {"Period": "DateTime", "Dim_Key": "Guid"}, object_key=None)
    with pytest.raises(ValueError, match="Ref_Key or Recorder"):
        rep.full_load("InformationRegister_Indep")


def test_read_object_keyset_url(monkeypatch):
    # Проверяем формирование URL keyset-страницы: $top, $orderby, $filter с guid-литералом.
    md = MetadataReader1C("http://x")
    reader = DataReader1C("http://x", md)
    captured = {}

    class _Resp:
        text = '<feed xmlns="http://www.w3.org/2005/Atom"></feed>'
        def raise_for_status(self): pass

    def fake_get(url, **kwargs):
        captured["url"] = url
        return _Resp()

    monkeypatch.setattr("cdc_1c.data_reader.requests.get", fake_get)

    key = uuid.UUID("11111111-1111-1111-1111-111111111111")
    reader.read_object("Catalog_X", top=500, after_key=key)
    url = captured["url"]
    assert "$top=500" in url and "$orderby=Ref_Key" in url
    # фильтр закодирован (пробелы → %20), guid-литерал сохранён.
    assert "$filter=Ref_Key%20gt%20guid'11111111-1111-1111-1111-111111111111'" in url

    # Регистр: ключ Recorder, строковый литерал (без guid'...').
    reader.read_object("AccumulationRegister_R", top=100, key_field="Recorder",
                       after_key=key, key_is_guid=False)
    url = captured["url"]
    assert "$orderby=Recorder" in url
    assert "$filter=Recorder%20gt%20'11111111-1111-1111-1111-111111111111'" in url


class _SyncExecutor:
    """Синхронный «пул»: submit выполняет задачу сразу (детерминированно, без потоков)."""
    def submit(self, fn, *args):
        fn(*args)


class _RecordingExecutor:
    """Записывает сабмиты, не выполняя их."""
    def __init__(self):
        self.submitted = []
    def submit(self, fn, *args):
        self.submitted.append(args)


def _flag_row(rep, name):
    t = rep.metadata.objects_table
    with rep.engine.connect() as c:
        return c.execute(select(t.c.full_load_is_required, t.c.last_full_load_dt)
                         .where(t.c.object_name == name)).first()


def test_dispatch_runs_full_load_and_marks_loaded():
    rep = _replicator()
    rep.metadata._sync_objects(["Catalog_X"])          # первая sync создаёт таблицу-реестр
    rep.metadata.require_full_load_if_new("Catalog_X")  # объект пришёл в пакете, не выгружался → нужно

    loaded = []
    rep.full_load = lambda name, batch_size=1000: loaded.append(name)
    rep._dispatch_full_loads(_SyncExecutor())

    assert loaded == ["Catalog_X"]                      # выгрузка запущена
    row = _flag_row(rep, "Catalog_X")
    assert row.full_load_is_required in (False, 0)       # mark_full_loaded снял требование
    assert row.last_full_load_dt is not None             # и проставил дату
    assert rep._full_load_in_progress == set()           # из «в работе» убрали


def test_dispatch_skips_in_progress():
    rep = _replicator()
    rep.metadata._sync_objects(["Catalog_X"])
    rep.metadata.require_full_load_if_new("Catalog_X")
    rep._full_load_in_progress.add("Catalog_X")          # уже выгружается

    ex = _RecordingExecutor()
    rep._dispatch_full_loads(ex)
    assert ex.submitted == []                            # повторно не сабмитим
