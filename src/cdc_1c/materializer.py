"""
DataMaterializer1C — инкрементальная материализация пользовательских вьюшек в persistent-таблицы.

Данные 1С сильно нормализованы; в DWH удобнее держать денормализованные таблицы, посчитанные из
вьюшек (join по нескольким таблицам 1С). Материализатор обновляет такую таблицу инкрементально —
только по ключам, изменившимся с прошлого прогона.

Как работает один прогон правила:
  1) prev = последний watermark по target из materializer_1c_log (нет → None = полная материализация);
  2) boundary = конец последней завершённой загрузки (max finished_at из replicator_1c_log);
  3) changed = UNION по триггер-таблицам: DISTINCT key_columns WHERE merged_on > prev;
  4) dbmerge мёрджит вьюшку в target, фильтруя источник: merge_key IN (changed) — всё в БД,
     без выгрузки в Python (dbmerge.exec(source_condition=...));
  5) при успехе пишем boundary как новый watermark в materializer_1c_log.

Материализатор не зависит от 1С — работает только БД→БД. Имена таблиц/колонок в правилах — уже
в виде, как они лежат в БД (после NameMapper). key_columns триггера = его object_key
(регистр → Recorder[+Recorder_Type], ТЧ/документ → Ref_Key), позиционно отображается на merge_key.
"""

import logging
from dataclasses import dataclass

from sqlalchemy import Engine, MetaData, Table, select, tuple_, union
from sqlalchemy.types import TypeEngine
from dbmerge import dbmerge

from cdc_1c.db_logs import Materializer1CLog, replicator_max_finished_at

logger = logging.getLogger(__name__)

MERGED_ON_FIELD = 'merged_on'


@dataclass
class TriggerTable:
    """Таблица-триггер: где ловим изменения (по merged_on) и какие колонки дают ключ."""
    table: str                 # имя таблицы в БД (уже транслитерированное)
    key_columns: list[str]     # = object_key таблицы; позиционно соответствует merge_key правила


@dataclass
class MaterializationRule:
    """Правило материализации: вьюшка → target-таблица по merge_key, триггеры — источники изменений."""
    target_table: str
    view: str
    merge_key: list[str]       # ключ во вьюшке/target (один столбец или составной)
    triggers: list[TriggerTable]
    # Типы колонок для создания target-таблицы. На Postgres типы выводятся из вьюшки автоматически
    # и это не нужно; на sqlite вычисляемые колонки вьюшки рефлектятся как NullType — для них тип
    # надо задать явно (имя колонки -> SQLAlchemy-тип).
    data_types: dict[str, TypeEngine] | None = None


class DataMaterializer1C:
    """
    Инкрементально материализует вьюшки в persistent-таблицы по списку правил.

    Принимает готовый engine и список правил (library-first, как Replicator1C). БД-целевая — Postgres;
    schema на sqlite приводится к None (как в dbmerge).
    """

    def __init__(self, engine: Engine, rules: list[MaterializationRule], schema: str | None = None):
        self.engine = engine
        self.rules = rules
        self.schema = None if engine.dialect.name == 'sqlite' else schema
        self.materializer_log = Materializer1CLog(engine, schema)

    def run(self) -> None:
        for rule in self.rules:
            self._materialize(rule)

    def _materialize(self, rule: MaterializationRule) -> None:
        prev = self.materializer_log.last_watermark(rule.target_table)
        boundary = replicator_max_finished_at(self.engine, self.schema)
        changed = self._changed_keys(rule, prev)

        with dbmerge(engine=self.engine, table_name=rule.target_table,
                     source_table_name=rule.view, source_schema=self.schema,
                     key=rule.merge_key, merged_on_field=MERGED_ON_FIELD,
                     data_types=rule.data_types,
                     delete_mode='no', schema=self.schema) as merge:
            source_keys = [merge.source_table.c[c] for c in rule.merge_key]
            changed_cols = list(changed.c)
            if len(source_keys) == 1:
                condition = source_keys[0].in_(select(changed_cols[0]))
            else:
                condition = tuple_(*source_keys).in_(select(*changed_cols))
            merge.exec(source_condition=condition)

        self.materializer_log.record(rule.target_table, boundary)
        logger.info("Materialized %s from view %s (changed since %s)",
                    rule.target_table, rule.view, prev)

    def _changed_keys(self, rule: MaterializationRule, prev):
        """
        Подзапрос изменившихся ключей: UNION по триггерам DISTINCT key_columns WHERE merged_on > prev.
        prev=None (первый прогон) → без фильтра по merged_on (берём все ключи = полная материализация).
        """
        md = MetaData()
        selects = []
        for trig in rule.triggers:
            table = Table(trig.table, md, schema=self.schema, autoload_with=self.engine)
            stmt = select(*[table.c[c] for c in trig.key_columns]).distinct()
            if prev is not None:
                stmt = stmt.where(table.c[MERGED_ON_FIELD] > prev)
            selects.append(stmt)
        combined = selects[0] if len(selects) == 1 else union(*selects)
        return combined.subquery()
