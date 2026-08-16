"""
Витрина: строки регистра «Заказы клиентов» с артикулом из справочника номенклатуры.

То же, что и materialize_example.py, но обработчиком: раннер зовёт его сам, когда в БД реально
изменился регистр или номенклатура, и передаёт окно (ctx.last_run_at, ctx.boundary].

Что даёт окно из ctx. В standalone-варианте границу приходилось выводить из самой целевой таблицы —
по отдельной отметке merged_on на каждый источник, потому что объекты 1С приезжают в обмене
независимо: номенклатура может доехать позже регистра, и ориентируясь только на merged_on регистра
такая строка навсегда осталась бы с NULL-артикулом. С окном из ctx этой бухгалтерии нет: одна
граница действует на все источники сразу, а `changed_since` собирает по ним условие.
"""

from sqlalchemy import select, tuple_
from dbmerge import dbmerge

from cdc_1c import Handler1C

DDL = """
CREATE OR REPLACE VIEW {schema}."ZakazyKlientov_view"
AS
(
SELECT
	s."Recorder",
	s."Recorder_Type",
	s."LineNumber",
	s."Zakazano" * (NOT s."is_deleted_or_empty")::int * --обнулим значение для удаленных строк
		(CASE WHEN s."RecordType"='Receipt' THEN 1 ELSE -1 END) --проверим тип операции + или -
    		"Zakazano",
	n."Code" "Artikul",
	s."merged_on",
	n."merged_on" "Nomenklatura_merged_on",
	s."is_deleted_or_empty"
FROM {schema}."AccumulationRegister_ZakazyKlientov" s
LEFT JOIN {schema}."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key"
);

CREATE TABLE IF NOT EXISTS {schema}."ZakazyKlientov" (
	"Recorder" uuid,
	"Recorder_Type" varchar,
	"LineNumber" int8,
	"Zakazano" numeric,
	"Artikul" varchar,
	"merged_on" timestamp,
	"Nomenklatura_merged_on" timestamp,
	"is_deleted_or_empty" boolean,
	CONSTRAINT "ZakazyKlientov_pkey"
		PRIMARY KEY ("Recorder", "Recorder_Type", "LineNumber")
);

-- Индекс по merged_on нужен не этой витрине, а той, что может строиться поверх неё: её обработчик
-- будет спрашивать «что здесь изменилось». Индекса по Nomenklatura_merged_on нет намеренно: он
-- существовал ради SELECT max(...) из самой витрины, а границу теперь приносит ctx.
CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientov_merged_on" ON
	{schema}."ZakazyKlientov" USING btree (merged_on);
"""


class ZakazyKlientov(Handler1C):
    # Читаются обе таблицы, поэтому обе в ON: по этому списку считается верхняя граница окна
    # (незавершённый merge любой из них прижимает границу к своему старту).
    ON = ["AccumulationRegister_ЗаказыКлиентов", "Catalog_Номенклатура"]

    def setup(self, ctx):
        # Один раз за процесс, а не на каждый вызов: полная выгрузка сигналит постранично, и
        # CREATE OR REPLACE VIEW на каждую страницу брал бы блокировки на пустом месте.
        self.execute(ctx, DDL)

    def handle(self, ctx):
        with dbmerge(ctx.engine, table_name="ZakazyKlientov", schema=ctx.schema,
                     source_table_name="ZakazyKlientov_view", source_schema=ctx.schema,
                     delete_mode='delete') as merge:

            # Единица пересчёта — не строка, а ГРУППА (Recorder), потому что удаляем мы тоже
            # группой. Берём ключи групп, у которых в окно попал хоть один источник.
            changed = merge.source_table.alias("chg")
            changed_recorders = (select(changed.c["Recorder"], changed.c["Recorder_Type"])
                                 .where(self.changed_since(ctx, changed.c["merged_on"],
                                                           changed.c["Nomenklatura_merged_on"]))
                                 .distinct())

            merge.exec(
                source_condition=tuple_(merge.source_table.c["Recorder"],
                                        merge.source_table.c["Recorder_Type"])
                                 .in_(changed_recorders),
                delete_condition=tuple_(merge.table.c["Recorder"], merge.table.c["Recorder_Type"])
                                 .in_(select(merge.temp_table.c["Recorder"],
                                             merge.temp_table.c["Recorder_Type"]).distinct()))
            # source_condition: тащим из вьюшки ЦЕЛИКОМ все строки изменившихся Recorder'ов. Нельзя
            # брать только строки, свежие сами по себе: если приехала одна номенклатура, «свежей»
            # станет лишь часть строк документа, а delete_condition снесёт все строки Recorder'а —
            # остальные не вернутся.
            # delete_condition: удаляем из целевой таблицы строки, для которых во временной таблице
            # есть ключ (Recorder, Recorder_Type), но нет строки с таким LineNumber — значит, в
            # источнике строку табличной части удалили. Если табличную часть удалили целиком,
            # прилетит строка с is_deleted_or_empty=True и почистит старые строки.
            #
            # На больших объёмах этот OR становится узким местом, и дело не в отсутствии индексов.
            # Ветки OR живут на РАЗНЫХ таблицах соединения, поэтому ни по одной из них нельзя
            # отфильтровать до джойна: условие проверяется только на готовой паре и вырождается в
            # Join Filter поверх полного соединения. BitmapOr из индексных сканов Postgres строит
            # только когда все ветки OR на одной таблице, а переписать OR в UNION через границу
            # джойна он не умеет. Лечится вручную: changed_recorders переписывается на UNION
            # отдельных запросов — по одному на источник, каждый ведётся своей отфильтрованной
            # таблицей и заходит через её индекс merged_on (их заводит репликатор, см.
            # DBWriter1C._ensure_merged_on_index). Вьюшка и остальная логика при этом прежние.

        ctx.logger.info("Витрина обновлена за окно (%s, %s]", self.since(ctx), ctx.boundary)
