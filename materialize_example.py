from sqlalchemy import create_engine,text,select,tuple_
from datetime import datetime
from dbmerge import dbmerge

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c")

sql = """
CREATE OR REPLACE VIEW cdc_1c_trade_demo."ZakazyKlientov_view"
AS
(
SELECT 
	s."Recorder",
	s."Recorder_Type",
	s."LineNumber",
	s."Zakazano" * (NOT s."is_deleted_or_empty")::int "Zakazano", --обнулим значение для удаленных строк
	n."Code" "Artikul",
	s."merged_on",
	s."is_deleted_or_empty"
FROM cdc_1c_trade_demo."AccumulationRegister_ZakazyKlientov" s
LEFT JOIN cdc_1c_trade_demo."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key" 
);

CREATE TABLE IF NOT EXISTS cdc_1c_trade_demo."ZakazyKlientov" (
	"Recorder" uuid,
	"Recorder_Type" varchar,
	"LineNumber" int8,
	"Zakazano" numeric,
	"Artikul" varchar,
	"merged_on" timestamp,
    "is_deleted_or_empty" boolean,
	CONSTRAINT "ZakazyKlientov_pkey" 
		PRIMARY KEY ("Recorder", "Recorder_Type", "LineNumber")	
);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientov_merged_on" ON 
	cdc_1c_trade_demo."ZakazyKlientov" USING btree (merged_on);
"""

with engine.begin() as conn:
    conn.execute(text(sql))

with engine.connect() as conn:
    max_merged_on = conn.scalar(text("""SELECT max(merged_on) FROM cdc_1c_trade_demo."ZakazyKlientov" """)) or datetime(2000, 1, 1)


with dbmerge(engine, table_name="ZakazyKlientov", schema="cdc_1c_trade_demo", 
             source_table_name="ZakazyKlientov_view", source_schema="cdc_1c_trade_demo",delete_mode='delete') as merge:
    merge.exec(source_condition=merge.source_table.c["merged_on"] > max_merged_on,
               delete_condition=tuple_(merge.table.c["Recorder"], merge.table.c["Recorder_Type"]).
                    in_(select(merge.temp_table.c["Recorder"], merge.temp_table.c["Recorder_Type"])))
    # source_condition: выбираем из вьюшки только новые записи (по merged_on > max_merged_on в целевой таблице).
    # delete_condition: удаляем из целевой таблицы все строки, для которых во временной таблицы есть ключ (Recorder, Recorder_Type).
    # но нет стоки с таким номером строки (LineNumber) — это значит, что в источнике была удалена строка табличной части, и её надо удалить из целевой таблицы.
    # если табличная часть была удалена целиком, то прилетит строка с is_deleted_or_empty=True и она почистит станые строки
    