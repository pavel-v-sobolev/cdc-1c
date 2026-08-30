"""
Оффлайн-тест постраничной полной выгрузки Replicator1C.full_load. Без живой 1С: чтение страниц
подменяется (read_object), проверяется логика цикла — keyset-пагинация (курсор after_values),
условие останова, сохранение в режиме полной выгрузки (full_load_started_at) для каждого объекта страницы
и одна строка в replicator_1c_log.
"""

import uuid
from datetime import date, datetime

import pytest
import requests
from sqlalchemy import select
from dbmerge import mergeResult

from cdc_1c import DataObject1C
from cdc_1c.data_reader import DataReader1C
from cdc_1c.metadata_reader import MetadataObject1C, MetadataReader1C
from cdc_1c.replicator import (FULL_LOAD_EMPTY_WINDOWS_TO_STOP,
                              FULL_LOAD_PARTITION_MAX_PAGES, Replicator1C)
from conftest import TEST_QUEUE_GUID

# Нулевой результат merge — writer.save в тестах замокан, но full_load агрегирует его результат.
_ZERO_RESULT = mergeResult(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)


def _response(status_code: int):
    """Минимальный requests.Response с нужным кодом — для HTTPError в стабах read_object."""
    response = requests.Response()
    response.status_code = status_code
    return response


def _replicator(db):
    engine = db.engine
    rep = Replicator1C(odata_url="http://x", odata_auth=None,
                       exchange_name="E", queue_guid=TEST_QUEUE_GUID, engine=engine, db_schema=db.schema)
    # Метаданные «уже загружены» — full_load не пойдёт в сеть; primary_key даёт $orderby.
    rep.metadata.is_loaded = True
    rep.metadata["Catalog_X"] = MetadataObject1C("Catalog_X", {"Ref_Key": "Guid"},
                                                 {"Ref_Key": "Guid"}, object_key=None)
    return rep


def test_full_load_paging(db, monkeypatch):
    # Справочник: ключ Ref_Key — ссылка, keyset по ней в 1С неприменим (см. _supports_keyset),
    # поэтому страницы берутся через $skip, а курсор after_values не используется.
    rep = _replicator(db)
    meta = rep.metadata["Catalog_X"]
    pages = iter([2, 2, 1])      # batch_size=2: две полные страницы и хвост → останов
    calls = []
    counter = {"v": 0}

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        calls.append({"top": top, "key_fields": key_fields, "after_values": after_values,
                      "key_types": key_types, "skip": skip})
        n = next(pages)
        keys = []
        for _ in range(n):       # детерминированно возрастающие Ref_Key
            counter["v"] += 1
            keys.append(uuid.UUID(int=counter["v"]))
        self.clear()
        self[object_name] = DataObject1C(meta, [{"Ref_Key": k} for k in keys])
        return n

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)

    saved = []
    def fake_save(name, obj, full_load_started_at=None):
        saved.append((name, obj.data_length, full_load_started_at))
        return _ZERO_RESULT
    rep.writer.save = fake_save

    rep.full_load("Catalog_X", batch_size=2)

    # $skip растёт на размер выданной страницы; keyset-курсор не задействован.
    assert [c["skip"] for c in calls] == [0, 2, 4]
    assert all(c["after_values"] is None for c in calls)
    assert all(c["top"] == 2 for c in calls)
    assert all(c["key_fields"] == ["Ref_Key"] and c["key_types"] == ["Guid"] for c in calls)
    # каждая страница сохранена в режиме полной выгрузки, и отметка у неё СВОЯ: guard'ы снимка
    # спрашивают «не переписали ли строку после того, как мы прочитали эти данные», а данные
    # читаются постранично. С общей отметкой на прогон группа, разрезанная границей страниц,
    # блокировала сама себя — её остаток молча не вставлялся. Всего 2+2+1 записей.
    assert all(s[2] is not None for s in saved)
    assert len({s[2] for s in saved}) == len(saved)
    assert [s[2] for s in saved] == sorted(s[2] for s in saved)
    assert sum(s[1] for s in saved) == 5

    # одна строка лога: это не пакет обмена (message_no=NULL), загрузка завершена (finished_at set).
    log = rep.replicator_log.table
    with rep.engine.connect() as conn:
        rows = conn.execute(select(log.c.object, log.c.message_no, log.c.finished_at)).all()
    assert len(rows) == 1
    assert rows[0].object == "Catalog_X" and rows[0].message_no is None
    assert rows[0].finished_at is not None


def test_full_load_register_paging(db, monkeypatch):
    # Регистр: сортировка по Recorder, но Recorder — ссылка, поэтому страницы через $skip.
    rep = _replicator(db)
    rep.metadata["AccumulationRegister_R"] = MetadataObject1C(
        "AccumulationRegister_R", {"Recorder": "Guid"},
        {"Recorder": "Guid", "LineNumber": "Int64", "Recorder_Type": "String"},
        object_key=["Recorder", "Recorder_Type"])
    meta = rep.metadata["AccumulationRegister_R"]
    pages = iter([2, 1])
    calls = []
    counter = {"v": 0}

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        calls.append({"key_fields": key_fields, "after_values": after_values,
                      "key_types": key_types, "skip": skip})
        n = next(pages)
        recs = []
        for _ in range(n):
            counter["v"] += 1
            recs.append(uuid.UUID(int=counter["v"]))
        self.clear()
        # две строки движений на регистратора — страница считается по entry (наборам), не по строкам.
        self[object_name] = DataObject1C(meta, [{"Recorder": r, "LineNumber": ln}
                                                for r in recs for ln in (1, 2)])
        return n

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    rep.full_load("AccumulationRegister_R", batch_size=2)

    assert all(c["key_fields"] == ["Recorder"] and c["key_types"] == ["String"] for c in calls)
    assert [c["skip"] for c in calls] == [0, 2]
    assert all(c["after_values"] is None for c in calls)


def test_full_load_key_recorder_key(db):
    # Регистр с единственным типом регистратора: 1С отдаёт Recorder_Key (Guid) вместо
    # Recorder/Recorder_Type — курсор всё равно должен быть по регистратору, а не составной.
    rep = _replicator(db)
    rep.metadata["InformationRegister_R"] = MetadataObject1C(
        "InformationRegister_R", {"Recorder_Key": "Guid", "Period": "DateTime"},
        {"Recorder_Key": "Guid", "Period": "DateTime"}, object_key=["Recorder_Key"])

    assert rep._full_load_key("InformationRegister_R") == (["Recorder_Key"], ["Guid"])


def _failing_above(limit, calls):
    """Стаб read_object: страницы больше limit сервер не отдаёт, остальные — пустые."""
    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        calls.append({"top": top, "skip": skip})
        if top > limit:
            raise requests.HTTPError("500", response=_response(500))
        self.clear()
        return 0
    return fake_read_object


def test_full_load_shrinks_to_single_entry(db, monkeypatch):
    # entry неделима (набор движений регистратора — мегабайты), поэтому уменьшаем вплоть до 1.
    # Смещение при этом не сдвигается: повторяем ту же страницу, а не следующую.
    rep = _replicator(db)
    calls = []
    monkeypatch.setattr(DataReader1C, "read_object", _failing_above(1, calls))
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    rep.full_load("Catalog_X", batch_size=1000)

    # старт — пробная страница (не batch_size), дальше деление на FULL_LOAD_BATCH_DIVISOR.
    assert [c["top"] for c in calls] == [20, 5, 1]
    assert all(c["skip"] == 0 for c in calls)


def test_full_load_remembers_reduced_page_size(db, monkeypatch):
    # Повторный прогон объекта начинает с уже подобранного размера, а не с пробной страницы.
    rep = _replicator(db)
    calls = []
    monkeypatch.setattr(DataReader1C, "read_object", _failing_above(4, calls))
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    rep.full_load("Catalog_X", batch_size=1000)
    rep.full_load("Catalog_X", batch_size=1000)

    assert [c["top"] for c in calls] == [20, 5, 1, 1]


def test_next_page_size_follows_response_weight(db):
    # Размер следующей страницы считается из фактического веса выданной: сколько entry
    # укладывается в бюджет FULL_LOAD_TARGET_BYTES (32 МБ).
    rep = _replicator(db)

    # запись ~1 МБ (набор движений регистратора) → 32 записи на страницу
    assert rep._next_page_size("O", 20, 20, 20 * 1024 * 1024, 1000) == 32
    # килобайтные записи справочника → упираемся в batch_size как в верхнюю границу
    assert rep._next_page_size("O", 20, 20, 20 * 1024, 1000) == 1000
    # одна запись тяжелее всего бюджета → берём по одной, ниже уже нельзя
    assert rep._next_page_size("O", 20, 1, 64 * 1024 * 1024, 1000) == 1
    # подобранный размер запоминается на объект
    assert rep._full_load_page_size["O"] == 1
    # пустая страница/неизвестный вес — размер не трогаем
    assert rep._next_page_size("O", 7, 0, 0, 1000) == 7


def test_full_load_reraises_permanent_error(db, monkeypatch):
    # 400/403/404 уменьшением страницы не лечатся — пробрасываем сразу, без повторов.
    rep = _replicator(db)

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        raise requests.HTTPError("403", response=_response(403))

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    with pytest.raises(requests.HTTPError):
        rep.full_load("Catalog_X", batch_size=1000)


def test_full_load_empty_object(db, monkeypatch):
    rep = _replicator(db)

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        self.clear()
        return 0     # объект пуст: первая же страница неполная → один запрос и останов

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    saved = []
    def fake_save(name, obj, full_load_started_at=None):
        saved.append(name)
        return _ZERO_RESULT
    rep.writer.save = fake_save

    rep.full_load("Catalog_X", batch_size=2)
    assert saved == []   # сохранять нечего, но лог-строка с finished_at должна появиться
    with rep.engine.connect() as conn:
        rows = conn.execute(select(rep.replicator_log.table.c.finished_at)).all()
    assert len(rows) == 1 and rows[0].finished_at is not None


def test_full_load_composite_key_with_reference(db, monkeypatch):
    # Независимый регистр, в ключе есть измерение-ссылка (Dim_Key) → keyset запрещён, идём $skip.
    rep = _replicator(db)
    meta = MetadataObject1C("InformationRegister_Indep", {"Period": "DateTime", "Dim_Key": "Guid"},
                            {"Period": "DateTime", "Dim_Key": "Guid"}, object_key=None)
    rep.metadata["InformationRegister_Indep"] = meta
    calls = []

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        calls.append({"key_fields": key_fields, "key_types": key_types,
                      "after_values": after_values, "skip": skip})
        self.clear()
        if len(calls) == 1:      # одна полная страница, затем пустая → останов
            self[object_name] = DataObject1C(meta, [
                {"Period": datetime(2026, 1, 1), "Dim_Key": uuid.UUID(int=1)},
                {"Period": datetime(2026, 1, 2), "Dim_Key": uuid.UUID(int=2)}])
            return 2
        return 0

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    # Границы периода тест не проверяет: объект мелкий, до нарезки дело не доходит
    # (см. Replicator1C._period_partitions).
    monkeypatch.setattr(DataReader1C, "read_date_bound", lambda *a, **k: None)
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    rep.full_load("InformationRegister_Indep", batch_size=2)

    assert calls[0]["key_fields"] == ["Period", "Dim_Key"]
    assert calls[0]["key_types"] == ["DateTime", "Guid"]
    assert [c["skip"] for c in calls] == [0, 2]
    assert all(c["after_values"] is None for c in calls)


def test_full_load_composite_key_keyset(db, monkeypatch):
    # Ключ без ссылок (Period + числовой код) — keyset корректен: курсор следующей страницы
    # = значения всех ключевых полей последней записи, $skip не используется.
    rep = _replicator(db)
    meta = MetadataObject1C("InformationRegister_Scalar", {"Period": "DateTime", "Code": "Int64"},
                            {"Period": "DateTime", "Code": "Int64"}, object_key=None)
    rep.metadata["InformationRegister_Scalar"] = meta
    calls = []

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        calls.append({"after_values": after_values, "skip": skip})
        self.clear()
        if len(calls) == 1:
            self[object_name] = DataObject1C(meta, [
                {"Period": datetime(2026, 1, 1), "Code": 1},
                {"Period": datetime(2026, 1, 2), "Code": 2}])
            return 2
        return 0

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    rep.full_load("InformationRegister_Scalar", batch_size=2)

    assert calls[0]["after_values"] is None
    assert calls[1]["after_values"] == [datetime(2026, 1, 2), 2]
    assert all(c["skip"] is None for c in calls)


def test_read_object_keyset_url(db, monkeypatch):
    # Проверяем формирование URL keyset-страницы: $top, $orderby, $filter с guid-литералом.
    md = MetadataReader1C("http://x")
    reader = DataReader1C("http://x", md)
    captured = {}

    class _Resp:
        ok = True
        text = '<feed xmlns="http://www.w3.org/2005/Atom"></feed>'
        content = text.encode()

    def fake_get(url, **kwargs):
        captured["url"] = url
        return _Resp()

    monkeypatch.setattr("cdc_1c.data_reader.requests.get", fake_get)

    key = uuid.UUID("11111111-1111-1111-1111-111111111111")
    reader.read_object("Catalog_X", top=500, key_fields=["Ref_Key"], after_values=[key],
                       key_types=["Guid"], use_keyset=True)
    url = captured["url"]
    assert "$top=500" in url and "$orderby=Ref_Key" in url
    # фильтр закодирован (пробелы → %20), guid-литерал сохранён.
    assert "$filter=Ref_Key%20gt%20guid'11111111-1111-1111-1111-111111111111'" in url

    # Регистр: ключ Recorder, строковый литерал (без guid'...').
    reader.read_object("AccumulationRegister_R", top=100, key_fields=["Recorder"],
                       after_values=[key], key_types=["String"], use_keyset=True)
    url = captured["url"]
    assert "$orderby=Recorder" in url
    assert "$filter=Recorder%20gt%20'11111111-1111-1111-1111-111111111111'" in url

    # $skip-страница: смещение в URL, $filter не появляется (курсора нет).
    reader.read_object("Catalog_X", top=500, key_fields=["Ref_Key"], skip=1000)
    url = captured["url"]
    assert "$top=500" in url and "$skip=1000" in url and "$orderby=Ref_Key" in url
    assert "$filter=" not in url
    # skip=0 (первая страница) в URL не пишем — 1С и так отдаёт выборку с начала.
    reader.read_object("Catalog_X", top=500, key_fields=["Ref_Key"], skip=0)
    assert "$skip" not in captured["url"]

    # Составной ключ: $orderby по всем полям, лексикографический фильтр с OR (в скобках).
    reader.read_object("InformationRegister_Indep", top=100,
                       key_fields=["Period", "Dim_Key"],
                       after_values=[datetime(2026, 1, 2), key],
                       key_types=["DateTime", "Guid"], use_keyset=True)
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


def test_dispatch_runs_full_load_and_marks_loaded(db):
    rep = _replicator(db)
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


def test_dispatch_skips_in_progress(db):
    rep = _replicator(db)
    rep.metadata._sync_objects(["Catalog_X"])
    rep.metadata.require_full_load_if_new("Catalog_X")
    rep._full_load_in_progress.add("Catalog_X")          # уже выгружается

    ex = _RecordingExecutor()
    rep._dispatch_full_loads(ex)
    assert ex.submitted == []                            # повторно не сабмитим


def test_build_date_filter(db):
    rep = _replicator(db)
    rep.metadata["Document_Y"] = MetadataObject1C(
        "Document_Y", {"Ref_Key": "Guid", "Date": "DateTime"}, {"Ref_Key": "Guid"})

    assert rep._build_date_filter("Document_Y", None, None, None) is None
    # нижняя граница — с начала дня (ge полночь)
    assert rep._build_date_filter("Document_Y", "Period", date(2026, 6, 1), None) \
        == "Period ge datetime'2026-06-01T00:00:00'"
    # верхняя граница — чистая дата → весь день целиком (lt полночь следующего дня)
    assert rep._build_date_filter("Document_Y", "Date", date(2026, 6, 1), date(2026, 6, 30)) \
        == "Date ge datetime'2026-06-01T00:00:00' and Date lt datetime'2026-07-01T00:00:00'"
    # верхняя граница — дата-время → как есть (le)
    assert rep._build_date_filter("Document_Y", "Date", None, datetime(2026, 6, 30, 15, 0, 0)) \
        == "Date le datetime'2026-06-30T15:00:00'"
    with pytest.raises(ValueError, match="date_field"):
        rep._build_date_filter("Document_Y", None, date(2026, 6, 1), None)


def test_build_date_filter_wraps_record_set_register(db):
    """
    У регистра, подчинённого регистратору, entry — это НАБОР записей, и поля Period на верхнем
    уровне нет: оно лежит внутри вложенной коллекции RecordSet. Плоское `Period ge ...` живая 1С
    отвергает с 400 «Сегмент пути Period не найден!», поэтому фильтр обязан быть лямбдой по
    коллекции.
    """
    rep = _replicator(db)
    rep.metadata["AccumulationRegister_R"] = MetadataObject1C(
        "AccumulationRegister_R", {"Recorder": "Guid", "Period": "DateTime"},
        {"Recorder": "Guid", "Recorder_Type": "String"},
        object_key=["Recorder", "Recorder_Type"])

    assert rep._is_record_set_object("AccumulationRegister_R")
    assert rep._build_date_filter("AccumulationRegister_R", "Period",
                                  date(2026, 6, 1), date(2026, 6, 30)) == (
        "RecordSet/any(r: r/Period ge datetime'2026-06-01T00:00:00' and "
        "r/Period lt datetime'2026-07-01T00:00:00')")

    # Табличная часть тоже имеет object_key (Ref_Key), но читается плоскими строками — её
    # оборачивать нельзя.
    rep.metadata["Document_Y_Tovary"] = MetadataObject1C(
        "Document_Y_Tovary", {"Ref_Key": "Guid", "Date": "DateTime"},
        {"Ref_Key": "Guid", "LineNumber": "Int64"}, object_key=["Ref_Key"], is_table_part=True)
    assert not rep._is_record_set_object("Document_Y_Tovary")
    assert rep._build_date_filter("Document_Y_Tovary", "Date", date(2026, 6, 1), None) \
        == "Date ge datetime'2026-06-01T00:00:00'"


def test_full_load_passes_date_filter(db, monkeypatch):
    # full_load транслирует date_field/date_from/date_to в extra_filter и отдаёт его в read_object.
    rep = _replicator(db)
    captured = {}

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        captured["extra_filter"] = extra_filter
        self.clear()
        return 0

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    # Границы периода тест не проверяет: объект мелкий, до нарезки дело не доходит
    # (см. Replicator1C._period_partitions).
    monkeypatch.setattr(DataReader1C, "read_date_bound", lambda *a, **k: None)
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    rep.full_load("Catalog_X", batch_size=2, date_field="Date",
                  date_from=date(2026, 6, 1), date_to=date(2026, 6, 30))
    # date_to — чистая дата → верхняя граница lt полночь следующего дня (весь 30-е включён).
    assert captured["extra_filter"] == \
        "Date ge datetime'2026-06-01T00:00:00' and Date lt datetime'2026-07-01T00:00:00'"


def test_read_object_combines_keyset_and_extra_filter(db, monkeypatch):
    # keyset-курсор и extra_filter объединяются в один $filter по AND; двоеточия в datetime сохранены.
    md = MetadataReader1C("http://x")
    reader = DataReader1C("http://x", md)
    captured = {}

    class _Resp:
        ok = True
        text = '<feed xmlns="http://www.w3.org/2005/Atom"></feed>'
        content = text.encode()

    monkeypatch.setattr("cdc_1c.data_reader.requests.get",
                        lambda url, **kw: captured.__setitem__("url", url) or _Resp())

    key = uuid.UUID("11111111-1111-1111-1111-111111111111")
    reader.read_object("Document_X", top=100, key_fields=["Ref_Key"], after_values=[key],
                       key_types=["Guid"], use_keyset=True,
                       extra_filter="Date ge datetime'2026-06-01T00:00:00'")
    url = captured["url"]
    assert "Ref_Key%20gt%20guid'11111111-1111-1111-1111-111111111111'" in url
    assert "%20and%20" in url
    assert "Date%20ge%20datetime'2026-06-01T00:00:00'" in url  # ':' оставлены как есть


def test_page_timestamp_is_clamped_to_unfinished_merges(db, monkeypatch):
    """
    Отметка страницы прижимается к незавершённым merge — той же границей, что и окно обработчика.

    merged_on штампуется ВНУТРИ merge-транзакции, а коммитится позже. Чужой merge, начавшийся до
    нашего чтения и закоммиченный после, оставил бы merged_on левее «сейчас»: guard счёл бы строку
    старой, и снимок затёр бы свежее изменение. Поэтому точка отсчёта — не «сейчас», а старт самого
    раннего живого незавершённого merge по таблицам этого объекта.
    """
    rep = _replicator(db)
    meta = rep.metadata["Catalog_X"]

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        self.clear()
        self[object_name] = DataObject1C(meta, [{"Ref_Key": uuid.UUID(int=1)}])
        return 1

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)

    saved = []
    rep.writer.save = lambda name, obj, full_load_started_at=None: (
        saved.append(full_load_started_at) or _ZERO_RESULT)

    table_name = rep._handler_key("Catalog_X")
    # Чужой merge в ту же таблицу, ещё не закоммиченный: его строки другим процессам не видны.
    with rep.writes.track(table_name) as tracked:
        rep.full_load("Catalog_X", batch_size=10)

    assert saved and all(s <= tracked.started_at for s in saved), (
        f"отметка страницы {saved} должна быть не позже старта незавершённого merge "
        f"{tracked.started_at}")


def _partitioned(db, monkeypatch, rows_per_page, *, date_field="Date"):
    """
    Репликатор с объектом, у которого есть дата, и подменённым чтением. Возвращает (rep, calls),
    где calls — список $filter каждой прочитанной страницы: по ним и видно нарезку.
    """
    rep = _replicator(db)
    meta = MetadataObject1C("Document_Y", {"Ref_Key": "Guid", date_field: "DateTime"},
                            {"Ref_Key": "Guid"}, object_key=None)
    rep.metadata["Document_Y"] = meta
    calls = []
    counter = {"v": 0}

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        calls.append(extra_filter)
        n = rows_per_page(extra_filter, skip or 0)
        keys = []
        for _ in range(n):
            counter["v"] += 1
            keys.append(uuid.UUID(int=counter["v"]))
        self.clear()
        self[object_name] = DataObject1C(meta, [{"Ref_Key": k} for k in keys])
        return n

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    monkeypatch.setattr(DataReader1C, "read_date_bound",
                        lambda self, name, field, *, newest, extra_filter=None:
                        datetime(2026, 3, 20) if newest else datetime(2026, 1, 15))
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT
    return rep, calls


def test_deep_object_is_read_by_day_windows_newest_first(db, monkeypatch):
    """
    Объект, который не дочитался за FULL_LOAD_PARTITION_MAX_PAGES страниц, перечитывается окнами
    по периоду — от свежих к старым.

    Смысл в $skip: без окон 1С на каждую страницу строит выборку заново, сортирует её целиком и
    отбрасывает первые N строк, поэтому цена растёт квадратично. Фильтр по периоду переводит
    запрос на индекс (Дата у документа входит в него), и сортируется маленький кусок. Порядок от
    свежих к старым выбран потому, что свежие данные нужнее: при обрыве прогона они уже в БД.

    Окна отмеряются В ДНЯХ, а не календарными месяцами, и стыкуются встык по полному datetime —
    время в границах не отбрасывается, иначе между окнами появилась бы дыра.
    """
    # Без фильтра страницы всегда полные (объект «бездонный»), с фильтром — одна строка.
    rep, calls = _partitioned(db, monkeypatch, lambda flt, skip: 1 if flt else 10)
    rep.full_load("Document_Y", batch_size=10)

    first_pass = [c for c in calls if c is None]
    assert len(first_pass) == FULL_LOAD_PARTITION_MAX_PAGES, \
        'сначала объект читается как есть и обрывается на лимите страниц'

    windows = [c for c in calls if c]
    # Границы известны (2026-01-15 .. 2026-03-20), поэтому обход начинается от самой поздней даты,
    # а не от «сейчас»: пустой промежуток до сегодняшнего дня окнами не перебирается.
    assert windows[0] == "Date ge datetime'2026-03-20T00:00:00'", \
        'первое окно открыто вверх: дата в будущем и строки, созданные во время прогона'

    # Дальше вниз шагами по FULL_LOAD_WINDOW_DAYS, встык.
    assert windows[1] == ("Date ge datetime'2026-02-18T00:00:00' and "
                          "Date lt datetime'2026-03-20T00:00:00'")
    assert windows[2] == ("Date ge datetime'2026-01-19T00:00:00' and "
                          "Date lt datetime'2026-02-18T00:00:00'")
    # Окно, накрывшее самую раннюю дату, — последнее: ниже ничего нет.
    assert windows[3] == ("Date ge datetime'2025-12-20T00:00:00' and "
                          "Date lt datetime'2026-01-19T00:00:00'")
    assert len(windows) == 4, 'ниже самой ранней даты окон быть не должно'

    # Окна стыкуются встык: правая граница каждого равна левой предыдущего — дыр нет.
    starts = [c.split("ge datetime'")[1][:19] for c in windows]
    ends = [c.split("lt datetime'")[1][:19] for c in windows[1:]]
    assert ends == starts[:-1]


def test_small_object_is_not_windowed(db, monkeypatch):
    """
    Мелкому объекту окна только вредят: он укладывается в пару страниц, а обход стоил бы запроса
    на каждое окно истории, даже пустое. Проверено на живой базе: 74 запроса против 2.
    """
    rep, calls = _partitioned(db, monkeypatch, lambda flt, skip: 1)
    rep.full_load("Document_Y", batch_size=10)

    assert calls == [None], 'объект дочитан с первого захода — окон быть не должно'


def test_deep_window_is_narrowed(db, monkeypatch):
    """
    Окно, которое само по себе даёт глубокий $skip, сужается и перечитывается с того же места —
    иначе обход окнами не спасает. Только сужение: обратно окно не растёт, иначе мы снова упёрлись
    бы в лимит и заплатили за перечитывание ещё раз.
    """
    deep = ("Date ge datetime'2026-02-18T00:00:00' and "
            "Date lt datetime'2026-03-20T00:00:00'")

    def rows(flt, skip):
        # Бездонны объект целиком и первое окно вниз; всё остальное — по одной странице.
        return 10 if flt is None or flt == deep else 1

    rep, calls = _partitioned(db, monkeypatch, rows)
    rep.full_load("Document_Y", batch_size=10)

    assert len([c for c in calls if c == deep]) == FULL_LOAD_PARTITION_MAX_PAGES, \
        'глубокое окно должно оборваться на лимите страниц, а не читаться до конца'

    windows = [c for c in calls if c]
    # 30 дней не поддались → 10 дней (30 // FULL_LOAD_WINDOW_DIVISOR), тот же правый край.
    assert ("Date ge datetime'2026-03-10T00:00:00' and "
            "Date lt datetime'2026-03-20T00:00:00'") in windows, \
        'окно обязано сузиться и перечитаться от той же правой границы'
    # и дальше идёт уже суженным шагом, не возвращаясь к 30 дням
    assert ("Date ge datetime'2026-02-28T00:00:00' and "
            "Date lt datetime'2026-03-10T00:00:00'") in windows


def test_window_keeps_time_of_day(db, monkeypatch):
    """
    Границы окон — полные datetime, без округления до суток. Дата в 1С хранится со временем, и
    окно, обрезанное до полуночи, либо оставило бы дыру, либо заставило перечитывать сутки.
    """
    rep, calls = _partitioned(db, monkeypatch, lambda flt, skip: 1 if flt else 10)
    monkeypatch.setattr(DataReader1C, "read_date_bound",
                        lambda self, name, field, *, newest, extra_filter=None:
                        datetime(2026, 3, 20, 14, 25, 37) if newest
                        else datetime(2026, 3, 1, 9, 5, 1))
    rep.full_load("Document_Y", batch_size=10)

    windows = [c for c in calls if c]
    assert windows[0] == "Date ge datetime'2026-03-20T14:25:37'", 'время верхней границы сохранено'
    assert windows[1] == ("Date ge datetime'2026-02-18T14:25:37' and "
                          "Date lt datetime'2026-03-20T14:25:37'"), \
        'шаг вниз сохраняет время суток — стык окон точный'


def test_deep_object_without_date_bounds_is_probed_and_read_through(db, monkeypatch):
    """
    Объект глубокий, но границ периода 1С не отдала (в первой строке не оказалось даты — см.
    read_date_bound). Раньше прогон на этом просто заканчивался: в логе «finished», в БД — ровно
    FULL_LOAD_PARTITION_MAX_PAGES страниц, а с mark_missing весь непрочитанный хвост объявлялся
    пропавшим.

    Сюда попадают только объекты, НЕ дочитавшиеся за лимит страниц, то есть заведомо непустые:
    значит отсутствие границы значит «границу взять не удалось», а не «данных нет». Конец истории
    нащупывается пустыми окнами, а остаток добирается хвостовым окном без нижней границы.
    """
    def rows(flt, skip):
        if flt is None:
            return 10                      # первый проход бездонный
        if ' ge ' not in flt:
            return 10 if skip < 120 else 3  # хвостовое окно: только верхняя граница
        return 0                            # пробные окна пусты
    rep, calls = _partitioned(db, monkeypatch, rows)
    monkeypatch.setattr(DataReader1C, "read_date_bound", lambda *a, **k: None)

    rep.full_load("Document_Y", batch_size=10)

    windows = [c for c in calls if c]
    probes = [c for c in windows if ' ge ' in c]
    # окно вверх + FULL_LOAD_EMPTY_WINDOWS_TO_STOP пустых окон вниз
    assert len(probes) == 1 + FULL_LOAD_EMPTY_WINDOWS_TO_STOP
    assert ' lt ' not in probes[0], 'первое окно открыто вверх'

    tail = [c for c in windows if ' ge ' not in c]
    assert len(tail) == 13, 'хвост дочитывается до конца сплошным $skip'
    assert tail[0].startswith('Date lt '), 'у хвостового окна только верхняя граница'


def test_record_set_register_is_read_by_lambda_windows(db, monkeypatch):
    """
    У регистра, подчинённого регистратору, дата лежит внутри вложенной коллекции RecordSet,
    поэтому окна строятся лямбдой `RecordSet/any(r: r/Period ...)`. Границы у такого объекта не
    спросить — `$orderby` по дате платформа молча игнорирует, — поэтому обход идёт пробными
    окнами от «сейчас» вниз.
    """
    rep = _replicator(db)
    rep.metadata["AccumulationRegister_R"] = MetadataObject1C(
        "AccumulationRegister_R", {"Recorder": "Guid", "Period": "DateTime"},
        {"Recorder": "Guid", "Recorder_Type": "String"},
        object_key=["Recorder", "Recorder_Type"])
    calls = []
    counter = {"v": 0}

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        calls.append(extra_filter)
        n = 10 if extra_filter is None else 0
        self.clear()
        rows = []
        for _ in range(n):
            counter["v"] += 1
            rows.append({"Recorder": uuid.UUID(int=counter["v"]), "Recorder_Type": "T"})
        if rows:
            self[object_name] = DataObject1C(rep.metadata["AccumulationRegister_R"], rows)
        return n

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    bounds = []
    monkeypatch.setattr(DataReader1C, "read_date_bound",
                        lambda *a, **k: bounds.append(1))     # звать её тут вообще нельзя
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    rep.full_load("AccumulationRegister_R", batch_size=10)

    assert bounds == [], 'у регистра границы не спрашивают — $orderby по дате он игнорирует'
    windows = [c for c in calls if c]
    assert windows, 'глубокий регистр обязан читаться окнами'
    assert all(c.startswith('RecordSet/any(r: ') and c.endswith(')') for c in windows), \
        f'фильтр окна обязан быть лямбдой по вложенной коллекции: {windows[:2]}'
    assert windows[0] == ("RecordSet/any(r: r/Period ge datetime'"
                          + windows[0].split("ge datetime'")[1][:19] + "')"), \
        'первое окно открыто вверх'


def test_page_size_does_not_climb_back_after_a_refusal(db, monkeypatch):
    """
    Отказ 1С ставит потолок, и подбор по весу его не перешагивает.

    Вес ответа причину отказа не объясняет: 1С валит сборку страницы во временных файлах, отдав
    перед этим лёгкий ответ. Без потолка первая же удавшаяся страница вернула бы размер к
    batch_size — и следующий запрос снова лёг бы: 500 → уменьшили → успех → вернулись → 500.
    """
    rep = _replicator(db)
    meta = rep.metadata["Catalog_X"]
    calls = []

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        calls.append(top)
        if top > 5:
            raise requests.HTTPError("500", response=_response(500))
        self.clear()
        # Ответ лёгкий: сам по себе он попросил бы у _next_page_size сразу batch_size.
        self.last_response_bytes = 1024
        if len(calls) < 6:
            self[object_name] = DataObject1C(
                meta, [{"Ref_Key": uuid.UUID(int=i)} for i in range(top)])
            return top
        return 0

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    rep.full_load("Catalog_X", batch_size=1000)

    assert calls[0] == 20 and calls[1] == 5, 'проба, затем деление на FULL_LOAD_BATCH_DIVISOR'
    assert max(calls[1:]) == 5, f'размер страницы не должен подниматься выше потолка: {calls}'
    assert rep._full_load_page_limit["Catalog_X"] == 5


def test_keyset_page_without_own_entries_fails_loudly(db, monkeypatch):
    """
    Страница вернула entry, но ни одна не легла в наш объект (все были неподдерживаемого класса и
    read_data_entries их пропустила). Курсор строить не из чего — раньше здесь был KeyError, теперь
    внятная ошибка. Молча оборваться нельзя: прогон отчитался бы успехом на половине объекта.
    """
    rep = _replicator(db)
    meta = MetadataObject1C("InformationRegister_Scalar", {"Period": "DateTime", "Code": "Int64"},
                            {"Period": "DateTime", "Code": "Int64"}, object_key=None)
    rep.metadata["InformationRegister_Scalar"] = meta

    def fake_read_object(self, object_name, top=None, key_fields=None, after_values=None,
                         key_types=None, extra_filter=None, skip=None, use_keyset=False):
        self.clear()
        return top          # полная страница, но объекта в reader нет

    monkeypatch.setattr(DataReader1C, "read_object", fake_read_object)
    rep.writer.save = lambda name, obj, full_load_started_at=None: _ZERO_RESULT

    with pytest.raises(RuntimeError, match="keyset cursor"):
        rep.full_load("InformationRegister_Scalar", batch_size=2)


def test_orderby_is_the_same_on_the_first_keyset_page_and_the_next(db, monkeypatch):
    """
    Порядок страниц выбирается по способу пагинации, а не по «есть ли уже курсор»: у первой
    keyset-страницы курсора нет, но $orderby у неё обязан быть тот же, что у следующих, — иначе
    курсор продолжил бы не тот порядок (дубли и пропуски на границах страниц).
    """
    md = MetadataReader1C("http://x")
    md["InformationRegister_Scalar"] = MetadataObject1C(
        "InformationRegister_Scalar", {"Period": "DateTime", "Code": "Int64", "Note": "String"},
        {"Period": "DateTime", "Code": "Int64"}, object_key=None)
    reader = DataReader1C("http://x", md)
    captured = []

    class _Resp:
        ok = True
        text = '<feed xmlns="http://www.w3.org/2005/Atom"></feed>'
        content = text.encode()

    monkeypatch.setattr("cdc_1c.data_reader.requests.get",
                        lambda url, **kw: (captured.append(url), _Resp())[1])

    # keyset: первая страница (курсора ещё нет) — сортировка ровно по ключу курсора.
    reader.read_object("InformationRegister_Scalar", top=10, key_fields=["Period"],
                       key_types=["DateTime"], use_keyset=True)
    assert "$orderby=Period&" in captured[-1] or captured[-1].endswith("$orderby=Period")

    # $skip: та же первая страница — сортировка дополняется остальными полями ключа,
    # иначе порядок внутри ничьей 1С не гарантирует (см. read_object).
    reader.read_object("InformationRegister_Scalar", top=10, key_fields=["Period"],
                       key_types=["DateTime"], skip=0)
    assert "$orderby=Period,Code" in captured[-1]


def test_window_title_and_filter(db):
    """Заголовок окна в логе и его $filter, включая открытые границы и режим набора записей."""
    from cdc_1c.replicator import _Window

    closed = _Window("Date", datetime(2026, 2, 1, 10, 30), datetime(2026, 3, 1))
    up = _Window("Date", datetime(2026, 3, 1), None)
    tail = _Window("Date", None, datetime(2026, 1, 1))

    assert closed.title == "[2026-02-01 10:30:00 .. 2026-03-01 00:00:00)"
    assert up.title == "[2026-03-01 00:00:00 .. +inf)"
    assert tail.title == "[-inf .. 2026-01-01 00:00:00)"

    assert closed.filter == ("Date ge datetime'2026-02-01T10:30:00' and "
                             "Date lt datetime'2026-03-01T00:00:00'")
    assert up.filter == "Date ge datetime'2026-03-01T00:00:00'"
    assert tail.filter == "Date lt datetime'2026-01-01T00:00:00'"
    # Окно без обеих границ фильтровать нечем.
    assert _Window("Date", None, None).filter is None

    # Режим набора записей: обе границы уходят ВНУТРЬ одной лямбды.
    reg = _Window("Period", datetime(2026, 2, 1), datetime(2026, 3, 1), record_set=True)
    assert reg.filter == ("RecordSet/any(r: r/Period ge datetime'2026-02-01T00:00:00' and "
                          "r/Period lt datetime'2026-03-01T00:00:00')")


def _mark_missing_spy(rep, monkeypatch):
    """Перехватывает шаг пометки и возвращает словарь с его аргументами: сама пометка здесь не
    интересна, интересен признак recheck — с ним прогон перепроверяет кандидатов в 1С."""
    captured = {}

    def spy(self, object_name, keys, started_at, reader, recheck, log_id=None):
        captured["recheck"] = recheck
        return 0

    monkeypatch.setattr(Replicator1C, "_mark_missing_rows", spy)
    return captured


def test_partitioned_run_rechecks_candidates(db, monkeypatch):
    """
    Нарезка читает объект окнами по дате — ровно так же, как пользовательский период, и порождает
    ту же проблему: строка не исчезла, а уехала. Окна идут от свежих к старым, поэтому
    документ, у которого во время прогона поменяли дату с февраля на апрель, не попал ни в
    апрельское чтение (тогда его там не было), ни в февральское (уже нет). Без перепроверки его
    пометили бы удалённым.
    """
    rep, calls = _partitioned(db, monkeypatch, lambda flt, skip: 1 if flt else 10)
    captured = _mark_missing_spy(rep, monkeypatch)

    rep.full_load("Document_Y", batch_size=10, mark_missing=True)

    assert [c for c in calls if c], 'объект должен был уйти в обход окнами'
    assert captured["recheck"] is True, 'окно создала нарезка — кандидатов надо перепроверить'


def test_run_without_any_window_does_not_recheck(db, monkeypatch):
    """Обратная сторона: объект прочитан сплошь, окна не было ни от пользователя, ни от нарезки —
    перепроверять нечего, и лишних запросов в 1С прогон не делает."""
    rep, calls = _partitioned(db, monkeypatch, lambda flt, skip: 1)
    captured = _mark_missing_spy(rep, monkeypatch)

    rep.full_load("Document_Y", batch_size=10, mark_missing=True)

    assert calls == [None], 'объект дочитан с первого захода'
    assert captured["recheck"] is False
