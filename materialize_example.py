from sqlalchemy import create_engine, text, select, tuple_, or_
from datetime import datetime
from dbmerge import dbmerge

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c")

# Ключевая идея: у КАЖДОЙ таблицы-источника своя отметка merged_on и своя граница обработки.
# Объекты 1С приезжают в обмене независимо, поэтому справочник номенклатуры может доехать (или
# измениться) позже регистра. Если ориентироваться только на merged_on регистра, такая строка уже не
# попадёт в инкремент и витрина навсегда останется с NULL/старым артикулом.
# Поэтому вьюшка отдаёт merged_on каждого источника отдельной колонкой, целевая таблица их хранит,
# а инкремент выбирается по «свежее хотя бы по одному источнику».
sql = """
CREATE OR REPLACE VIEW cdc_1c_trade_demo."ZakazyKlientov_view"
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
	s."merged_on",                              --отметка регистра
	n."merged_on" "Nomenklatura_merged_on",     --отметка справочника (NULL, пока он не доехал)
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
	"Nomenklatura_merged_on" timestamp,
    "is_deleted_or_empty" boolean,
	CONSTRAINT "ZakazyKlientov_pkey"
		PRIMARY KEY ("Recorder", "Recorder_Type", "LineNumber")
);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientov_merged_on" ON
	cdc_1c_trade_demo."ZakazyKlientov" USING btree (merged_on);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientov_Nomenklatura_merged_on" ON
	cdc_1c_trade_demo."ZakazyKlientov" USING btree ("Nomenklatura_merged_on");
"""

with engine.begin() as conn:
    conn.execute(text(sql))

# Граница обработки — своя по каждому источнику. Целевая таблица сама себе хранит, до какого момента
# по каждому из них она посчитана. NULL (источник ещё ни разу не приезжал) = «с начала времён».
with engine.connect() as conn:
    row = conn.execute(text("""SELECT max("merged_on"), max("Nomenklatura_merged_on")
                               FROM cdc_1c_trade_demo."ZakazyKlientov" """)).one()
max_merged_on = row[0] or datetime(2000, 1, 1)
max_nomenklatura_merged_on = row[1] or datetime(2000, 1, 1)


with dbmerge(engine, table_name="ZakazyKlientov", schema="cdc_1c_trade_demo",
             source_table_name="ZakazyKlientov_view", source_schema="cdc_1c_trade_demo",delete_mode='delete') as merge:

    # Единица пересчёта — не строка, а ГРУППА (Recorder), потому что удаляем мы тоже группой.
    # Берём ключи групп, где свежее стал хоть один источник.
    changed = merge.source_table.alias("chg")
    changed_recorders = (select(changed.c["Recorder"], changed.c["Recorder_Type"])
                         .where(or_(changed.c["merged_on"] > max_merged_on,
                                    changed.c["Nomenklatura_merged_on"] > max_nomenklatura_merged_on))
                         .distinct())

    merge.exec(source_condition=tuple_(merge.source_table.c["Recorder"], merge.source_table.c["Recorder_Type"]).
                    in_(changed_recorders),
               delete_condition=tuple_(merge.table.c["Recorder"], merge.table.c["Recorder_Type"]).
                    in_(select(merge.temp_table.c["Recorder"], merge.temp_table.c["Recorder_Type"]).distinct()))
    # source_condition: тащим из вьюшки ЦЕЛИКОМ все строки изменившихся Recorder'ов. Нельзя брать
    # только строки, свежие сами по себе: если приехала одна номенклатура, «свежей» станет лишь часть
    # строк документа, а delete_condition снесёт все строки Recorder'а — остальные не вернутся.
    # delete_condition: удаляем из целевой таблицы все строки, для которых во временной таблице есть
    # ключ (Recorder, Recorder_Type), но нет строки с таким номером строки (LineNumber) — это значит,
    # что в источнике была удалена строка табличной части, и её надо удалить из целевой таблицы.
    # если табличная часть была удалена целиком, то прилетит строка с is_deleted_or_empty=True и она почистит станые строки
    #
    # На больших объёмах OR поверх JOIN'а планировщик не разложит по индексам. Тогда changed_recorders
    # переписывается на UNION отдельных запросов — по одному на источник, каждый заходит через свой
    # ix_*_merged_on, — а вьюшка и остальная логика остаются прежними.
