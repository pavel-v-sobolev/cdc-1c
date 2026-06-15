"""
Оффлайн-тесты DataMaterializer1C на sqlite.

- контролируемый сценарий (ручные таблицы с merged_on + лог) — проверяет полную и инкрементальную
  материализацию (только изменившиеся ключи) и составной merge_key;
- прогон поверх replay-корпуса (реальные данные 1С через фейковый сервер) — интеграционная проверка
  с рефлексией вьюшки и data_types.

Без маркера integration — запускается везде (без 1С/Postgres).
"""

import sqlite3
import uuid
from datetime import datetime

from sqlalchemy import Integer, create_engine, text

import fake_1c  # соседний модуль в tests/
from cdc_1c import DataMaterializer1C, MaterializationRule, Replicator1C, TriggerTable
from cdc_1c.db_logs import Replicator1CLog

sqlite3.register_adapter(uuid.UUID, str)

# ISO с микросекундами — формат, в котором SQLAlchemy хранит DateTime на sqlite (чтобы сравнение
# merged_on с watermark было консистентным).
T0 = "2026-01-01 10:00:00.000000"   # начальная загрузка
T1 = "2026-01-02 10:00:00.000000"   # инкрементальная загрузка (позже T0)


def _log_finished(engine, finished_at: str):
    """Записать в replicator_1c_log завершённую загрузку с контролируемым finished_at."""
    ts = datetime.fromisoformat(finished_at)
    log = Replicator1CLog(engine)
    with engine.begin() as conn:
        conn.execute(log.table.insert().values(
            exchange="E", object="orders", message_no=1, started_at=ts, finished_at=ts))


def _build_orders_scenario(engine):
    with engine.begin() as c:
        c.execute(text('CREATE TABLE orders ("Ref_Key" TEXT, descr TEXT, merged_on TIMESTAMP)'))
        c.execute(text('CREATE TABLE lines ("Ref_Key" TEXT, qty INTEGER, merged_on TIMESTAMP)'))
        c.execute(text('INSERT INTO orders VALUES '
                       f"('A','order A','{T0}'),('B','order B','{T0}')"))
        c.execute(text('INSERT INTO lines VALUES '
                       f"('A',1,'{T0}'),('A',2,'{T0}'),('B',5,'{T0}')"))
        c.execute(text('CREATE VIEW v_orders AS '
                       'SELECT o."Ref_Key" AS "Ref_Key", o.descr AS descr, '
                       '(SELECT COALESCE(SUM(qty),0) FROM lines l WHERE l."Ref_Key"=o."Ref_Key") AS total_qty '
                       'FROM orders o'))
    _log_finished(engine, T0)


ORDERS_RULE = MaterializationRule(
    target_table="mart_orders",
    view="v_orders",
    merge_key=["Ref_Key"],
    triggers=[TriggerTable("orders", ["Ref_Key"]), TriggerTable("lines", ["Ref_Key"])],
    data_types={"total_qty": Integer()},
)


def _mart(engine):
    with engine.connect() as c:
        return {r[0]: (r[1], r[2]) for r in
                c.execute(text('SELECT "Ref_Key", descr, total_qty FROM mart_orders')).fetchall()}


def test_full_then_incremental(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'c.db'}")
    _build_orders_scenario(engine)
    mat = DataMaterializer1C(engine, [ORDERS_RULE], schema=None)

    # Полная материализация (watermark пуст → все ключи).
    mat.run()
    assert _mart(engine) == {"A": ("order A", 3), "B": ("order B", 5)}

    # Инкремент: заказ A переехал новой загрузкой (merged_on=T1), а к B добавлена строка СТАРОЙ
    # загрузкой (merged_on=T0 ≤ watermark) — B не должен попасть в инкремент.
    with engine.begin() as c:
        c.execute(text(f"UPDATE orders SET descr='order A v2', merged_on='{T1}' WHERE \"Ref_Key\"='A'"))
        c.execute(text(f"INSERT INTO lines VALUES ('B',100,'{T0}')"))   # stale, вне watermark
    _log_finished(engine, T1)

    mat.run()
    mart = _mart(engine)
    assert mart["A"] == ("order A v2", 3)         # A пересчитан
    assert mart["B"] == ("order B", 5)            # B НЕ тронут (его 100 проигнорировано) → инкремент работает

    # Полный прогон (сброс watermark) подхватил бы и B — проверим, что данные на месте.
    with engine.connect() as c:
        wm = c.execute(text("SELECT count(*) FROM materializer_1c_log")).scalar()
    assert wm == 2   # две записи лога материализации (полный + инкремент)


def test_composite_merge_key(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'c.db'}")
    with engine.begin() as c:
        c.execute(text('CREATE TABLE reg ("Recorder" TEXT, "Recorder_Type" TEXT, qty INTEGER, merged_on TIMESTAMP)'))
        c.execute(text('INSERT INTO reg VALUES '
                       f"('r1','Doc',10,'{T0}'),('r1','Doc',5,'{T0}'),('r2','Doc',7,'{T0}')"))
        c.execute(text('CREATE VIEW v_reg AS '
                       'SELECT "Recorder","Recorder_Type", SUM(qty) AS total FROM reg '
                       'GROUP BY "Recorder","Recorder_Type"'))
    _log_finished(engine, T0)

    rule = MaterializationRule(
        target_table="mart_reg", view="v_reg",
        merge_key=["Recorder", "Recorder_Type"],
        triggers=[TriggerTable("reg", ["Recorder", "Recorder_Type"])],
        data_types={"total": Integer()},
    )
    DataMaterializer1C(engine, [rule], schema=None).run()
    with engine.connect() as c:
        rows = {(r[0], r[1]): r[2] for r in
                c.execute(text('SELECT "Recorder","Recorder_Type",total FROM mart_reg')).fetchall()}
    assert rows == {("r1", "Doc"): 15, ("r2", "Doc"): 7}


def test_materialize_over_replay(tmp_path):
    """Реальные данные 1С через фейковый сервер → загрузка → материализация заказов."""
    engine = create_engine(f"sqlite:///{tmp_path / 'c.db'}")
    with fake_1c.running_server("tests/responses/trade_demo_8.5") as (base_url, fake):
        repl = Replicator1C(odata_url=base_url, odata_user="x", odata_password="y",
                            exchange_name="E", queue_guid=fake.queue_guid,
                            engine=engine, db_schema=None)
        repl.run_forever(interval=0, max_iterations=len(fake.batches))

    with engine.begin() as c:
        c.execute(text('CREATE VIEW v_zakaz AS SELECT z."Ref_Key" AS "Ref_Key", '
                       '(SELECT COUNT(*) FROM "Document_ZakazKlienta_Tovary" t '
                       ' WHERE t."Ref_Key"=z."Ref_Key") AS line_count '
                       'FROM "Document_ZakazKlienta" z'))

    rule = MaterializationRule(
        target_table="mart_zakaz", view="v_zakaz", merge_key=["Ref_Key"],
        triggers=[TriggerTable("Document_ZakazKlienta", ["Ref_Key"]),
                  TriggerTable("Document_ZakazKlienta_Tovary", ["Ref_Key"])],
        data_types={"line_count": Integer()},
    )
    DataMaterializer1C(engine, [rule], schema=None).run()

    with engine.connect() as c:
        n_orders = c.execute(text('SELECT COUNT(*) FROM "Document_ZakazKlienta"')).scalar()
        n_mart = c.execute(text("SELECT COUNT(*) FROM mart_zakaz")).scalar()
    assert n_mart == n_orders > 0
