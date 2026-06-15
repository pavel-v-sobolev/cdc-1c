"""
Лог-таблицы пайплайна в БД (общие хелперы на SQLAlchemy Core).

- `replicator_1c_log` — лог загрузки: строка на объект (exchange, object, message_no) с временами
  начала/окончания (серверное `func.now()`). finished_at=NULL у незавершённой/упавшей загрузки.
- `materializer_1c_log` — лог материализации: target-таблица, время merge и watermark (до какого
  конца загрузки обработали).

Время берётся серверным `func.now()` (на sqlite SQLAlchemy компилирует в CURRENT_TIMESTAMP).
Схема на sqlite не поддерживается — приводится к None (как в dbmerge).
"""

import logging
from datetime import datetime

from sqlalchemy import (Column, DateTime, Engine, Integer, MetaData, String,
                        Table, func, insert, select, update)

logger = logging.getLogger(__name__)

REPLICATOR_LOG = "replicator_1c_log"
MATERIALIZER_LOG = "materializer_1c_log"


def _effective_schema(engine: Engine, schema: str | None) -> str | None:
    return None if engine.dialect.name == "sqlite" else schema


def _replicator_log_table(metadata: MetaData, schema: str | None) -> Table:
    return Table(
        REPLICATOR_LOG, metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("exchange", String),
        Column("object", String),
        Column("message_no", Integer),
        Column("started_at", DateTime),
        Column("finished_at", DateTime, nullable=True),
        schema=schema,
    )


def _materializer_log_table(metadata: MetaData, schema: str | None) -> Table:
    return Table(
        MATERIALIZER_LOG, metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("target_table", String),
        Column("propagated_at", DateTime),
        Column("watermark", DateTime, nullable=True),
        schema=schema,
    )


class Replicator1CLog:
    """Лог загрузки (replicator_1c_log): start() при начале объекта, finish() при успехе."""

    def __init__(self, engine: Engine, schema: str | None = None):
        self.engine = engine
        self.schema = _effective_schema(engine, schema)
        self.table = _replicator_log_table(MetaData(), self.schema)
        self.table.create(engine, checkfirst=True)

    def start(self, exchange: str, obj: str, message_no: int | None) -> int:
        with self.engine.begin() as conn:
            res = conn.execute(insert(self.table).values(
                exchange=exchange, object=obj, message_no=message_no,
                started_at=func.now()))
            return res.inserted_primary_key[0]

    def finish(self, log_id: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.id == log_id)
                         .values(finished_at=func.now()))


def replicator_max_finished_at(engine: Engine, schema: str | None = None) -> datetime | None:
    """Конец последней завершённой загрузки = max(finished_at) из replicator_1c_log.

    Граница watermark для материализатора. Таблицу создаём checkfirst (если загрузок ещё не было —
    вернётся None).
    """
    table = _replicator_log_table(MetaData(), _effective_schema(engine, schema))
    table.create(engine, checkfirst=True)
    with engine.connect() as conn:
        return conn.execute(select(func.max(table.c.finished_at))).scalar()


class Materializer1CLog:
    """Лог материализации (materializer_1c_log): чтение/запись watermark по target-таблице."""

    def __init__(self, engine: Engine, schema: str | None = None):
        self.engine = engine
        self.schema = _effective_schema(engine, schema)
        self.table = _materializer_log_table(MetaData(), self.schema)
        self.table.create(engine, checkfirst=True)

    def last_watermark(self, target_table: str) -> datetime | None:
        with self.engine.connect() as conn:
            return conn.execute(
                select(self.table.c.watermark)
                .where(self.table.c.target_table == target_table)
                .order_by(self.table.c.id.desc())
                .limit(1)).scalar()

    def record(self, target_table: str, watermark: datetime | None) -> None:
        with self.engine.begin() as conn:
            conn.execute(insert(self.table).values(
                target_table=target_table, propagated_at=func.now(), watermark=watermark))
