from sqlalchemy import create_engine, text, select
from datetime import datetime
from dbmerge import dbmerge

engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/cdc_1c")

# Вьюшка с ДРУГИМ ключом (агрегат по Number+Artikul, а не по строкам регистра).
#
# Строим её в два слоя, и это главный приём примера:
#   1) _rows_view — построчный слой: все JOIN'ы описаны ОДИН раз, и здесь же каждый источник отдаёт
#      свой merged_on ОТДЕЛЬНОЙ колонкой. Объекты 1С приезжают в обмене независимо: справочник
#      номенклатуры или сам документ могут доехать позже регистра. Если ориентироваться только на
#      merged_on регистра, такая строка уже не попадёт в инкремент — витрина навсегда останется со
#      старым артикулом, а группа, ждавшая документа (INNER JOIN), не появится вовсе.
#   2) _view — агрегат поверх построчного слоя: MAX(merged_on) по каждому источнику на группу.
#      Целевая таблица сама себе хранит, до какого момента по каждому источнику она посчитана.
# Тот же _rows_view даёт и список изменившихся групп — логика JOIN'ов не дублируется.
sql = """
CREATE OR REPLACE VIEW cdc_1c_trade_demo."ZakazyKlientovGrouped_rows_view"
AS
(
SELECT
	z."Number",
	COALESCE(n."Code",'') "Artikul",
	s."Zakazano" * (NOT s."is_deleted_or_empty")::int *  --обнулим значение для удаленных строк
		(CASE WHEN s."RecordType"='Receipt' THEN 1 ELSE -1 END) --проверим тип операции + или -
			"Zakazano",
	s."merged_on",                              --отметка регистра
	z."merged_on" "ZakazKlienta_merged_on",     --отметка документа
	n."merged_on" "Nomenklatura_merged_on"      --отметка справочника (NULL, пока он не доехал)
FROM cdc_1c_trade_demo."AccumulationRegister_ZakazyKlientov" s
INNER JOIN cdc_1c_trade_demo."Document_ZakazKlienta" z ON z."Ref_Key" = s."Recorder" AND
	s."Recorder_Type"='Document_ЗаказКлиента'
LEFT JOIN cdc_1c_trade_demo."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key"
);

CREATE OR REPLACE VIEW cdc_1c_trade_demo."ZakazyKlientovGrouped_view"
AS
(
SELECT
	"Number",
	"Artikul",
	SUM("Zakazano") "Zakazano",
	MAX("merged_on") "merged_on",                                          --границы обработки группы:
	MAX("ZakazKlienta_merged_on") "ZakazKlienta_merged_on",                --самое свежее изменение
	MAX("Nomenklatura_merged_on") "Nomenklatura_merged_on"                 --по каждому источнику
FROM cdc_1c_trade_demo."ZakazyKlientovGrouped_rows_view"
GROUP BY "Number","Artikul"
);

CREATE TABLE IF NOT EXISTS cdc_1c_trade_demo."ZakazyKlientovGrouped" (
	"Number" varchar,
	"Artikul" varchar,
	"Zakazano" numeric,
	"merged_on" timestamp,
	"ZakazKlienta_merged_on" timestamp,
	"Nomenklatura_merged_on" timestamp,
	CONSTRAINT "ZakazyKlientovGrouped_pkey"
		PRIMARY KEY ("Number", "Artikul")
);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_merged_on" ON
	cdc_1c_trade_demo."ZakazyKlientovGrouped" USING btree (merged_on);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_ZakazKlienta_merged_on" ON
	cdc_1c_trade_demo."ZakazyKlientovGrouped" USING btree ("ZakazKlienta_merged_on");

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_Nomenklatura_merged_on" ON
	cdc_1c_trade_demo."ZakazyKlientovGrouped" USING btree ("Nomenklatura_merged_on");

CREATE INDEX IF NOT EXISTS "ix_Document_ZakazKlienta_Number" ON
	cdc_1c_trade_demo."Document_ZakazKlienta" USING btree ("Number");
"""

with engine.begin() as conn:
    conn.execute(text(sql))

# Границы обработки берём из самой целевой таблицы (а не из ZakazyKlientov) — по одной на источник.
# NULL (источник ещё ни разу не приезжал) = «с начала времён».
with engine.connect() as conn:
    row = conn.execute(text("""SELECT max("merged_on"), max("ZakazKlienta_merged_on"),
                                      max("Nomenklatura_merged_on")
                               FROM cdc_1c_trade_demo."ZakazyKlientovGrouped" """)).one()
max_merged_on = row[0] or datetime(2000, 1, 1)
max_zakaz_merged_on = row[1] or datetime(2000, 1, 1)
max_nomenklatura_merged_on = row[2] or datetime(2000, 1, 1)

# Какие группы (Number) изменились с прошлого прогона — спрашиваем у построчного слоя: свежее стал
# хоть один источник. Никакого повторения JOIN'ов: одна вьюшка — один источник правды.
changed_numbers = text(
    """SELECT DISTINCT "Number"
       FROM cdc_1c_trade_demo."ZakazyKlientovGrouped_rows_view"
       WHERE "merged_on" > :max_merged_on
          OR "ZakazKlienta_merged_on" > :max_zakaz_merged_on
          OR "Nomenklatura_merged_on" > :max_nomenklatura_merged_on"""
).bindparams(max_merged_on=max_merged_on,
             max_zakaz_merged_on=max_zakaz_merged_on,
             max_nomenklatura_merged_on=max_nomenklatura_merged_on)

with dbmerge(engine, table_name="ZakazyKlientovGrouped", schema="cdc_1c_trade_demo",
             source_table_name="ZakazyKlientovGrouped_view", source_schema="cdc_1c_trade_demo",
             delete_mode='delete') as merge:
    merge.exec(
        # source_condition — это WHERE по вьюшке ZakazyKlientovGrouped_view.
        # Фильтруем по ключу группы (Number), т.к. merged_on во вьюшке агрегированы и фильтровать
        # по ним «строки источника» нельзя — у группы другой ключ.
        source_condition=merge.source_table.c["Number"].in_(changed_numbers),
        # delete_condition: чистим все строки изменившихся Number в целевой таблице, затем merge
        # вставит актуальный состав. Удаляем по Number (а не по PK), чтобы убрать и те Artikul,
        # что выпали из группы (в том числе при переименовании Code номенклатуры).
        delete_condition=merge.table.c["Number"].in_(select(merge.temp_table.c["Number"])),
    )

# На больших объёмах OR поверх JOIN'а планировщик не разложит по индексам. Тогда changed_numbers
# переписывается на UNION отдельных запросов — по одному на источник, каждый заходит через свой
# ix_*_merged_on, — а вьюшки и остальная логика остаются прежними.
