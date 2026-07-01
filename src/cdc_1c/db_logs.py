"""
Лог-таблицы пайплайна в БД (общие хелперы на SQLAlchemy Core).

- `replicator_1c_log` — лог загрузки: строка на объект (exchange, object, message_no) с временами
  начала/окончания (серверное `func.now()`). finished_at=NULL у незавершённой/упавшей загрузки.

Время берётся серверным `func.now()` (на sqlite SQLAlchemy компилирует в CURRENT_TIMESTAMP).
Схема на sqlite не поддерживается — приводится к None (как в dbmerge).
"""

import logging
from dbmerge import mergeResult

from sqlalchemy import (Column, DateTime, Engine, Integer, MetaData, String,
                        Table, func, insert, update, schema, Numeric)

logger = logging.getLogger(__name__)

REPLICATOR_LOG = "replicator_1c_log"


def _check_create_schema(engine: Engine, schema_name: str | None) -> str | None:
    if engine.dialect.name not in ['sqlite']:
        with engine.begin() as conn:
            if not conn.dialect.has_schema(conn, schema_name):
                logger.info(f"""Creating schema "{schema_name}".""")
                conn.execute(schema.CreateSchema(schema_name))
            return schema_name
    else:
        return None



def _replicator_log_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        REPLICATOR_LOG, metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("exchange", String),
        Column("object", String),
        Column("message_no", Integer),
        Column("started_at", DateTime),
        Column("finished_at", DateTime, nullable=True),
        Column("inserted_row_count", Integer, nullable=True),
        Column("updated_row_count", Integer, nullable=True),
        Column("deleted_row_count", Integer, nullable=True),
        Column("total_time", Numeric, nullable=True),
        schema=schema_name,
    )


class Replicator1CLog:
    """Лог загрузки (replicator_1c_log): start() при начале объекта, finish() при успехе."""

    def __init__(self, engine: Engine, schema_name: str | None = None):
        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema_name)
        self.table = _replicator_log_table(MetaData(), self.schema_name)
        self.table.create(engine, checkfirst=True)

    def start(self, exchange: str, obj: str, message_no: int | None) -> int:
        with self.engine.begin() as conn:
            res = conn.execute(insert(self.table).values(
                exchange=exchange, object=obj, message_no=message_no,
                started_at=func.now()))
            return res.inserted_primary_key[0]

    def finish(self, log_id: int, result: mergeResult) -> None:
        with self.engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.id == log_id)
                         .values(finished_at=func.now(), 
                                 inserted_row_count = result.inserted_row_count,
                                 updated_row_count = result.updated_row_count,
                                 deleted_row_count = result.deleted_row_count,
                                 total_time = result.total_time))
