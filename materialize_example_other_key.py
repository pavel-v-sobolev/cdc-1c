from sqlalchemy import create_engine, text, select
from datetime import datetime
from dbmerge import dbmerge

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c")

# Вьюшка с ДРУГИМ ключом (агрегат по Number+Artikul, а не по строкам регистра).
# В неё дополнительно тащим MAX(merged_on) на группу — это и есть граница обработки: целевая таблица
# сама себе хранит, до какого конца загрузки она посчитана (как в materialize_example.py).
sql = """
CREATE OR REPLACE VIEW cdc_1c_trade_demo."ZakazyKlientovGrouped_view"
AS
(
SELECT
	z."Number",
	COALESCE(n."Code",'') "Artikul",
	SUM(s."Zakazano" * (NOT s."is_deleted_or_empty")::int *  --обнулим значение для удаленных строк
		CASE WHEN s."RecordType"='Receipt' THEN 1 ELSE -1 END) --проверим тип операции + или -
			"Zakazano",
	MAX(s."merged_on") "merged_on"   --граница обработки группы = самое свежее изменение её строк
FROM cdc_1c_trade_demo."AccumulationRegister_ZakazyKlientov" s
INNER JOIN cdc_1c_trade_demo."Document_ZakazKlienta" z ON z."Ref_Key" = s."Recorder" AND
	s."Recorder_Type"='Document_ЗаказКлиента'
LEFT JOIN cdc_1c_trade_demo."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key"
GROUP BY z."Number",n."Code"
);

CREATE TABLE IF NOT EXISTS cdc_1c_trade_demo."ZakazyKlientovGrouped" (
	"Number" varchar,
	"Artikul" varchar,
	"Zakazano" numeric,
	"merged_on" timestamp,
	CONSTRAINT "ZakazyKlientovGrouped_pkey"
		PRIMARY KEY ("Number", "Artikul")
);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_merged_on" ON
	cdc_1c_trade_demo."ZakazyKlientovGrouped" USING btree (merged_on);
    
CREATE INDEX IF NOT EXISTS "ix_Document_ZakazKlienta_Number" ON
	cdc_1c_trade_demo."Document_ZakazKlienta" USING btree ("Number");
"""

with engine.begin() as conn:
    conn.execute(text(sql))

# Границу обработки берём из самой целевой таблицы (а не из ZakazyKlientov).
with engine.connect() as conn:
    max_merged_on = conn.scalar(
        text("""SELECT max(merged_on) FROM cdc_1c_trade_demo."ZakazyKlientovGrouped" """)
    ) or datetime(2000, 1, 1)

# Какие группы (Number) изменились с прошлого прогона: ищем по базовым таблицам, где merged_on есть.
# Сам подзапрос — обычным SQL через text().
changed_numbers = text(
    """SELECT z."Number"
       FROM cdc_1c_trade_demo."AccumulationRegister_ZakazyKlientov" s
       INNER JOIN cdc_1c_trade_demo."Document_ZakazKlienta" z ON z."Ref_Key" = s."Recorder" AND
            s."Recorder_Type"='Document_ЗаказКлиента'
       WHERE s."merged_on" > :max_merged_on"""
).bindparams(max_merged_on=max_merged_on)

with dbmerge(engine, table_name="ZakazyKlientovGrouped", schema="cdc_1c_trade_demo",
             source_table_name="ZakazyKlientovGrouped_view", source_schema="cdc_1c_trade_demo",
             delete_mode='delete') as merge:
    merge.exec(
        # source_condition — это WHERE по вьюшке ZakazyKlientovGrouped_view.
        # Фильтруем по ключу группы (Number), т.к. merged_on во вьюшке агрегирован и фильтровать
        # по нему «строки источника» нельзя — у группы другой ключ.
        source_condition=merge.source_table.c["Number"].in_(changed_numbers),
        # delete_condition: чистим все строки изменившихся Number в целевой таблице, затем merge
        # вставит актуальный состав. Удаляем по Number (а не по PK), чтобы убрать и те Artikul,
        # что выпали из группы.
        delete_condition=merge.table.c["Number"].in_(select(merge.temp_table.c["Number"])),
    )
