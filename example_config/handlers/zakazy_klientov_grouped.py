"""
Витрина с ДРУГИМ ключом: агрегат по (Номер документа, Артикул), а не по строкам регистра.
Версия materialize_example_other_key.py под контракт обработчиков.

Приём тот же, что и в standalone-примере — два слоя вьюшек:
  1) _rows_view — построчный слой: все JOIN'ы описаны ОДИН раз, и здесь же каждый источник отдаёт
     свой merged_on отдельной колонкой. Он же отвечает на вопрос «какие группы изменились».
  2) _view — агрегат поверх построчного слоя, он и есть источник merge.
Граница обработки приходит готовой в ctx.last_run_at — одна на все источники, — поэтому целевой
таблице не нужно хранить по отметке на источник, чтобы вычислить её из самой себя.
"""

from sqlalchemy import select, text

from dbmerge import dbmerge

from cdc_1c import Handler1C

DDL = """
CREATE OR REPLACE VIEW {schema}."ZakazyKlientovGrouped_rows_view"
AS
(
SELECT
	z."Number",
	COALESCE(n."Code",'') "Artikul",
	s."Zakazano" * (NOT s."is_deleted_or_empty")::int *  --обнулим значение для удаленных строк
		(CASE WHEN s."RecordType"='Receipt' THEN 1 ELSE -1 END) --проверим тип операции + или -
			"Zakazano",
	s."merged_on",
	z."merged_on" "ZakazKlienta_merged_on",
	n."merged_on" "Nomenklatura_merged_on"
FROM {schema}."AccumulationRegister_ZakazyKlientov" s
INNER JOIN {schema}."Document_ZakazKlienta" z ON z."Ref_Key" = s."Recorder" AND
	s."Recorder_Type"='Document_ЗаказКлиента'
LEFT JOIN {schema}."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key"
);

CREATE OR REPLACE VIEW {schema}."ZakazyKlientovGrouped_view"
AS
(
SELECT
	"Number",
	"Artikul",
	SUM("Zakazano") "Zakazano",
	MAX("merged_on") "merged_on"
FROM {schema}."ZakazyKlientovGrouped_rows_view"
GROUP BY "Number","Artikul"
);

CREATE TABLE IF NOT EXISTS {schema}."ZakazyKlientovGrouped" (
	"Number" varchar,
	"Artikul" varchar,
	"Zakazano" numeric,
	"merged_on" timestamp,
	CONSTRAINT "ZakazyKlientovGrouped_pkey"
		PRIMARY KEY ("Number", "Artikul")
);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_merged_on" ON
	{schema}."ZakazyKlientovGrouped" USING btree (merged_on);

CREATE INDEX IF NOT EXISTS "ix_Document_ZakazKlienta_Number" ON
	{schema}."Document_ZakazKlienta" USING btree ("Number");
"""

# Какие группы (Number) попали в окно — спрашиваем у построчного слоя: свежее стал хоть один
# источник. Никакого повторения JOIN'ов: одна вьюшка — один источник правды.
CHANGED_NUMBERS = """
SELECT DISTINCT "Number"
FROM {schema}."ZakazyKlientovGrouped_rows_view"
WHERE "merged_on" > :since
   OR "ZakazKlienta_merged_on" > :since
   OR "Nomenklatura_merged_on" > :since
"""


class ZakazyKlientovGrouped(Handler1C):
    ON = ["AccumulationRegister_ЗаказыКлиентов", "Document_ЗаказКлиента", "Catalog_Номенклатура"]

    def setup(self, ctx):
        self.execute(ctx, DDL)

    def handle(self, ctx):
        # Здесь граница подставляется в текстовый SQL, а не собирается changed_since: условие живёт
        # в построчном слое, до которого выражениям SQLAlchemy из этого места не дотянуться.
        changed_numbers = text(CHANGED_NUMBERS.format(schema=self.schema_prefix(ctx))).bindparams(
            since=self.since(ctx))

        with dbmerge(ctx.engine, table_name="ZakazyKlientovGrouped", schema=ctx.schema,
                     source_table_name="ZakazyKlientovGrouped_view", source_schema=ctx.schema,
                     delete_mode='delete') as merge:
            merge.exec(
                # source_condition — это WHERE по вьюшке ZakazyKlientovGrouped_view. Фильтруем по
                # ключу группы (Number), т.к. merged_on во вьюшке агрегирован и фильтровать по нему
                # «строки источника» нельзя — у группы другой ключ.
                source_condition=merge.source_table.c["Number"].in_(changed_numbers),
                # delete_condition: чистим все строки изменившихся Number в целевой таблице, затем
                # merge вставит актуальный состав. Удаляем по Number (а не по PK), чтобы убрать и
                # те Artikul, что выпали из группы (в том числе при переименовании Code).
                delete_condition=merge.table.c["Number"].in_(select(merge.temp_table.c["Number"])),
            )

        # На больших объёмах этот OR становится узким местом, и дело не в отсутствии индексов.
        # Ветки OR живут на РАЗНЫХ таблицах соединения, поэтому ни по одной из них нельзя
        # отфильтровать до джойна: условие проверяется только на готовой паре и вырождается в
        # Join Filter поверх полного соединения (в EXPLAIN — три Seq Scan и Rows Removed by Join
        # Filter). BitmapOr из индексных сканов Postgres строит только когда все ветки OR на одной
        # таблице, а преобразовывать OR в UNION через границу джойна он не умеет.
        #
        # Лечится вручную: CHANGED_NUMBERS переписывается на UNION отдельных запросов — по одному
        # на источник. Тогда каждая ветка ведётся своей отфильтрованной таблицей и заходит через её
        # индекс merged_on (их заводит сам репликатор, см. DBWriter1C._ensure_merged_on_index), а
        # стоимость начинает зависеть от размера окна, а не от размера таблиц. Ветке, которую ведёт
        # маленький справочник, нужен вдобавок индекс по колонке соединения (Nomenklatura_Key в
        # регистре) — иначе она всё равно пойдёт по регистру целиком. Вьюшки при этом не меняются.
        ctx.logger.info("Витрина обновлена за окно (%s, %s]", self.since(ctx), ctx.boundary)
