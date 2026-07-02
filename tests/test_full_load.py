"""
Оффлайн-тест постраничной полной выгрузки Replicator1C.full_load. Без живой 1С: чтение страниц
подменяется (read_object), проверяется логика цикла — keyset-пагинация (курсор after_values),
условие останова, сохранение в режиме полной выгрузки (full_load=True) для каждого объекта страницы
и одна строка в replicator_1c_log.
"""

import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, select
from dbmerge import mergeResult

from cdc_1c import DataObject1C
from cdc_1c.data_reader import DataReader1C
from cdc_1c.metadata_reader import MetadataObject1C, MetadataReader1C
from cdc_1c.replicator import Replicator1C

# Нулевой результат merge — writer.save в тестах замокан, но full_load агрегирует его результат.
_ZERO_RESULT = mergeResult(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)


def _replicator():
    engine = create_engine("sqlite://")  # in-memory
    rep = Replicator1C(odata_url="http://x", odata_auth=None,
                       exchange_name="E", queue_guid="Q", engine=engine)
    # Метаданные «уже загружены» — full_load не пойдёт в сеть; primary_key даёт $orderby.
    rep.metadata.is_loaded = True
    rep.metadata["Catalog_X"] = MetadataObject1C("Catalog_X", {"Ref_Key": "Guid"},
                                                 {"Ref_Key": "Guid"}, object_key=None)
    return rep


def test_full_load_paging(monkeypatch):
    rep = _replicator()
    meta = rep.metadata["Catalog_X"]
    pages = iter([2, 2, 1])      # batch_size=2: две полные страницы и хвост → останов
    calls = []
    page_last_key = []           # последний Ref_Key каждой выданной страницы (= ожидаемый курсор)
    counter = {"v": 0}

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None):
        calls.append({"top": top, "key_fields": key_fields, "after_values": after_values,
                      "key_types": key_types})
        n = next(pages)
        keys = []
        for _ in range(n):       # детерминированно возрастающие Ref_Key
            counter["v"] += 1
            keys.append(uuid.UUID(int=counter["v"]))
        self.clear()
        self[object_name] = DataObject1C(meta, [{"Ref_Key": k} for k in keys])
        page_last_key.append([keys[-1]] if keys else None)   # after_values — список значений ключа
        return n

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)

    saved = []
    def fake_save(name, obj, full_load=False):
        saved.append((name, obj.data_length, full_load))
        return _ZERO_RESULT
    rep.writer.save = fake_save

    rep.full_load("Catalog_X", batch_size=2)

    # keyset: первый запрос без курсора, далее after_values = ключ последней записи предыдущей страницы;
    # лимит top=batch_size, ключ [Ref_Key]/[Guid]; останов на неполной странице (3 запроса).
    assert [c["after_values"] for c in calls] == [None, page_last_key[0], page_last_key[1]]
    assert all(c["top"] == 2 for c in calls)
    assert all(c["key_fields"] == ["Ref_Key"] and c["key_types"] == ["Guid"] for c in calls)
    # каждая страница сохранена в режиме полной выгрузки (full_load=True); всего 2+2+1 записей.
    assert [s[2] for s in saved] == [True, True, True]
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
        "AccumulationRegister_R", {"Recorder": "Guid"},
        {"Recorder": "Guid", "LineNumber": "Int64", "Recorder_Type": "String"},
        object_key=["Recorder", "Recorder_Type"])
    meta = rep.metadata["AccumulationRegister_R"]
    pages = iter([2, 1])
    calls = []
    page_last_key = []
    counter = {"v": 0}

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None):
        calls.append({"key_fields": key_fields, "after_values": after_values, "key_types": key_types})
        n = next(pages)
        recs = []
        for _ in range(n):
            counter["v"] += 1
            recs.append(uuid.UUID(int=counter["v"]))
        self.clear()
        # две строки движений на регистратора — курсор всё равно берётся по последней записи.
        self[object_name] = DataObject1C(meta, [{"Recorder": r, "LineNumber": ln}
                                                for r in recs for ln in (1, 2)])
        page_last_key.append([recs[-1]] if recs else None)
        return n

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, full_load=False: _ZERO_RESULT

    rep.full_load("AccumulationRegister_R", batch_size=2)

    assert all(c["key_fields"] == ["Recorder"] and c["key_types"] == ["String"] for c in calls)
    assert [c["after_values"] for c in calls] == [None, page_last_key[0]]


def test_full_load_empty_object(monkeypatch):
    rep = _replicator()

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None):
        self.clear()
        return 0     # объект пуст: первая же страница неполная → один запрос и останов

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    saved = []
    def fake_save(name, obj, full_load=False):
        saved.append(name)
        return _ZERO_RESULT
    rep.writer.save = fake_save

    rep.full_load("Catalog_X", batch_size=2)
    assert saved == []   # сохранять нечего, но лог-строка с finished_at должна появиться
    with rep.engine.connect() as conn:
        rows = conn.execute(select(rep.replicator_log.table.c.finished_at)).all()
    assert len(rows) == 1 and rows[0].finished_at is not None


def test_full_load_composite_key(monkeypatch):
    # Независимый регистр (нет Ref_Key/Recorder) — составной keyset по всему первичному ключу;
    # курсор следующей страницы = значения всех ключевых полей последней записи.
    rep = _replicator()
    meta = MetadataObject1C("InformationRegister_Indep", {"Period": "DateTime", "Dim_Key": "Guid"},
                            {"Period": "DateTime", "Dim_Key": "Guid"}, object_key=None)
    rep.metadata["InformationRegister_Indep"] = meta
    calls = []

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None):
        calls.append({"key_fields": key_fields, "key_types": key_types, "after_values": after_values})
        self.clear()
        if len(calls) == 1:      # одна полная страница, затем пустая → останов
            self[object_name] = DataObject1C(meta, [
                {"Period": datetime(2026, 1, 1), "Dim_Key": uuid.UUID(int=1)},
                {"Period": datetime(2026, 1, 2), "Dim_Key": uuid.UUID(int=2)}])
            return 2
        return 0

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, full_load=False: _ZERO_RESULT

    rep.full_load("InformationRegister_Indep", batch_size=2)

    assert calls[0]["key_fields"] == ["Period", "Dim_Key"]
    assert calls[0]["key_types"] == ["DateTime", "Guid"]
    assert calls[0]["after_values"] is None
    # курсор 2-й страницы — значения ключа последней строки 1-й страницы (все поля составного ключа).
    assert calls[1]["after_values"] == [datetime(2026, 1, 2), uuid.UUID(int=2)]


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
    reader.read_object("Catalog_X", top=500, key_fields=["Ref_Key"], after_values=[key],
                       key_types=["Guid"])
    url = captured["url"]
    assert "$top=500" in url and "$orderby=Ref_Key" in url
    # фильтр закодирован (пробелы → %20), guid-литерал сохранён.
    assert "$filter=Ref_Key%20gt%20guid'11111111-1111-1111-1111-111111111111'" in url

    # Регистр: ключ Recorder, строковый литерал (без guid'...').
    reader.read_object("AccumulationRegister_R", top=100, key_fields=["Recorder"],
                       after_values=[key], key_types=["String"])
    url = captured["url"]
    assert "$orderby=Recorder" in url
    assert "$filter=Recorder%20gt%20'11111111-1111-1111-1111-111111111111'" in url

    # Составной ключ: $orderby по всем полям, лексикографический фильтр с OR (в скобках).
    reader.read_object("InformationRegister_Indep", top=100,
                       key_fields=["Period", "Dim_Key"],
                       after_values=[datetime(2026, 1, 2), key],
                       key_types=["DateTime", "Guid"])
    url = captured["url"]
    assert "$orderby=Period,Dim_Key" in url
    assert "Period%20gt%20datetime'2026-01-02T00:00:00'" in url
    assert "%20or%20" in url
    assert "Period%20eq%20datetime'2026-01-02T00:00:00'%20and%20Dim_Key%20gt%20guid'11111111-1111-1111-1111-111111111111'" in url


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


def test_build_date_filter():
    from cdc_1c.replicator import Replicator1C
    assert Replicator1C._build_date_filter(None, None, None) is None
    # нижняя граница — с начала дня (ge полночь)
    assert Replicator1C._build_date_filter("Period", date(2026, 6, 1), None) \
        == "Period ge datetime'2026-06-01T00:00:00'"
    # верхняя граница — чистая дата → весь день целиком (lt полночь следующего дня)
    assert Replicator1C._build_date_filter("Date", date(2026, 6, 1), date(2026, 6, 30)) \
        == "Date ge datetime'2026-06-01T00:00:00' and Date lt datetime'2026-07-01T00:00:00'"
    # верхняя граница — дата-время → как есть (le)
    assert Replicator1C._build_date_filter("Date", None, datetime(2026, 6, 30, 15, 0, 0)) \
        == "Date le datetime'2026-06-30T15:00:00'"
    with pytest.raises(ValueError, match="date_field"):
        Replicator1C._build_date_filter(None, date(2026, 6, 1), None)


def test_full_load_passes_date_filter(monkeypatch):
    # full_load транслирует date_field/date_from/date_to в extra_filter и отдаёт его в read_object.
    rep = _replicator()
    captured = {}

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None):
        captured["extra_filter"] = extra_filter
        self.clear()
        return 0

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, full_load=False: _ZERO_RESULT

    rep.full_load("Catalog_X", batch_size=2, date_field="Date",
                  date_from=date(2026, 6, 1), date_to=date(2026, 6, 30))
    # date_to — чистая дата → верхняя граница lt полночь следующего дня (весь 30-е включён).
    assert captured["extra_filter"] == \
        "Date ge datetime'2026-06-01T00:00:00' and Date lt datetime'2026-07-01T00:00:00'"


def test_read_object_combines_keyset_and_extra_filter(monkeypatch):
    # keyset-курсор и extra_filter объединяются в один $filter по AND; двоеточия в datetime сохранены.
    md = MetadataReader1C("http://x")
    reader = DataReader1C("http://x", md)
    captured = {}

    class _Resp:
        text = '<feed xmlns="http://www.w3.org/2005/Atom"></feed>'
        def raise_for_status(self): pass

    monkeypatch.setattr("cdc_1c.data_reader.requests.get",
                        lambda url, **kw: captured.__setitem__("url", url) or _Resp())

    key = uuid.UUID("11111111-1111-1111-1111-111111111111")
    reader.read_object("Document_X", top=100, key_fields=["Ref_Key"], after_values=[key],
                       key_types=["Guid"], extra_filter="Date ge datetime'2026-06-01T00:00:00'")
    url = captured["url"]
    assert "Ref_Key%20gt%20guid'11111111-1111-1111-1111-111111111111'" in url
    assert "%20and%20" in url
    assert "Date%20ge%20datetime'2026-06-01T00:00:00'" in url  # ':' оставлены как есть
