"""
Витрина с ДРУГИМ ключом: агрегат по (Номер документа, Год, Артикул), а не по строкам регистра.
Второй пример: как та же схема работает, когда ключ витрины не совпадает с ключом источника.

Ключ группы здесь СОСТАВНОЙ — (Number, Year), — и этим пример отличается от zakazy_klientov.py,
где группа задаётся одним регистратором. Составной ключ меняет ровно две вещи: запрос «что
изменилось» отдаёт две колонки, а фильтры собираются row-value сравнением
`tuple_(a, b).in_(...)`. Год берётся из даты документа, то есть это ВЫЧИСЛЯЕМАЯ часть ключа —
см. предупреждение в handle().

Смена ключа требует двух слоёв вьюшек:
  1) _rows_view — построчный слой: все JOIN'ы описаны ОДИН раз, и здесь же каждый источник отдаёт
     свой merged_on отдельной колонкой. Он же отвечает на вопрос «какие группы изменились».
     Оба соединения — LEFT: объекты 1С приезжают в обмене независимо, и движения регистра могут
     доехать раньше своего документа или своей номенклатуры. INNER JOIN просто спрятал бы такие
     строки до прибытия документа; LEFT показывает их сразу, но ценой NULL в ключе группы —
     отсюда COALESCE ниже и уборка группы-заглушки в handle().
  2) _view — агрегат поверх построчного слоя, он и есть источник merge.
Граница обработки приходит готовой в ctx.last_run_at — одна на все источники, — поэтому целевой
таблице не нужно хранить по отметке на источник, чтобы вычислить её из самой себя.
"""

from sqlalchemy import or_, select, text, tuple_

from dbmerge import dbmerge

from cdc_1c import Handler1C

DDL = """
CREATE OR REPLACE VIEW {schema}."ZakazyKlientovGrouped_rows_view"
AS
(
SELECT
	COALESCE(z."Number",'') "Number",
	COALESCE(EXTRACT(YEAR FROM z."Date")::int, 0) "Year",
	COALESCE(n."Code",'') "Artikul",
	s."Zakazano" * (NOT s."is_deleted_or_empty")::int *  --обнулим значение для удаленных строк
		(CASE WHEN s."RecordType"='Receipt' THEN 1 ELSE -1 END) --проверим тип операции + или -
			"Zakazano",
	s."merged_on",
	z."merged_on" "ZakazKlienta_merged_on",
	n."merged_on" "Nomenklatura_merged_on"
FROM {schema}."AccumulationRegister_ZakazyKlientov" s
LEFT JOIN {schema}."Document_ZakazKlienta" z ON z."Ref_Key" = s."Recorder" AND
	s."Recorder_Type"='Document_ЗаказКлиента'
LEFT JOIN {schema}."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key"
);

CREATE OR REPLACE VIEW {schema}."ZakazyKlientovGrouped_view"
AS
(
SELECT
	"Number",
	"Year",
	"Artikul",
	SUM("Zakazano") "Zakazano",
	MAX("merged_on") "merged_on"
FROM {schema}."ZakazyKlientovGrouped_rows_view"
GROUP BY "Number","Year","Artikul"
);

CREATE TABLE IF NOT EXISTS {schema}."ZakazyKlientovGrouped" (
	"Number" varchar,
	"Year" int4,
	"Artikul" varchar,
	"Zakazano" numeric,
	"merged_on" timestamp,
	CONSTRAINT "ZakazyKlientovGrouped_pkey"
		PRIMARY KEY ("Number", "Year", "Artikul")
);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_merged_on" ON
	{schema}."ZakazyKlientovGrouped" USING btree (merged_on);

-- Индекс под source_condition: по ключу группы фильтруется агрегатная вьюшка, а Number приходит
-- из документа.
CREATE INDEX IF NOT EXISTS "ix_Document_ZakazKlienta_Number" ON
	{schema}."Document_ZakazKlienta" USING btree ("Number");

-- Индекс по колонке соединения со справочником: без него ветка UNION ALL, которую ведёт свежая
-- номенклатура, дотягивается до регистра полным сканом. Репликатор такие индексы не создаёт — он
-- заводит только merged_on, а что с чем соединяется, знает витрина, а не он.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Nomenklatura_Key" ON
	{schema}."AccumulationRegister_ZakazyKlientov" USING btree ("Nomenklatura_Key");
"""

# Какие группы (Number, Year) стали свежее — спрашиваем у построчного слоя: одна вьюшка, один источник
# правды, JOIN'ы не дублируются.
#
# Ветка на источник через UNION ALL, а не один WHERE с OR — почему именно так, см. комментарий
# в handle(). DISTINCT не нужен: результат уходит в IN (...), дубликаты там роли не играют.
CHANGED_GROUPS = """
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped_rows_view"
 WHERE "merged_on" > :since
UNION ALL
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped_rows_view"
 WHERE "ZakazKlienta_merged_on" > :since
UNION ALL
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped_rows_view"
 WHERE "Nomenklatura_merged_on" > :since
"""


class ZakazyKlientovGrouped(Handler1C):
    ON = ["AccumulationRegister_ЗаказыКлиентов", "Document_ЗаказКлиента", "Catalog_Номенклатура"]

    def setup(self, ctx):
        self.execute(ctx, DDL)

    def handle(self, ctx):
        # Здесь граница подставляется в текстовый SQL, а не собирается changed_since: условие живёт
        # в построчном слое, до которого выражениям SQLAlchemy из этого места не дотянуться.
        changed_groups = text(CHANGED_GROUPS.format(schema=self.schema_prefix(ctx))).bindparams(
            since=self.since(ctx))

        with dbmerge(ctx.engine, table_name="ZakazyKlientovGrouped", schema=ctx.schema,
                     source_table_name="ZakazyKlientovGrouped_view", source_schema=ctx.schema,
                     delete_mode='delete') as merge:
            # Ключ группы составной, поэтому сравнение row-value: (Number, Year) IN (SELECT
            # Number, Year ...). В остальном ничего не меняется по сравнению с одиночным ключом.
            source_key = tuple_(merge.source_table.c["Number"], merge.source_table.c["Year"])
            target_key = tuple_(merge.table.c["Number"], merge.table.c["Year"])

            # Группа-заглушка: строки, чей документ ещё не доехал (Number='' из COALESCE). Её
            # приходится пересчитывать всегда, и вот почему. Когда документ наконец приезжает, его
            # строки МЕНЯЮТ ключ группы — уходят из ('', 0) в реальный (Number, Year). Запрос
            # изменившихся групп вернёт только новый ключ, старый в него не попадёт, и строки под
            # заглушкой остались бы в витрине навсегда. Та же ловушка сработала бы, если бы дата
            # документа переехала через границу года: у составного ключа с вычисляемой частью
            # строка может мигрировать между группами, и покинутую группу надо чистить отдельно.
            # Здесь это дёшево: ключ заглушки один и известен заранее.
            source_placeholder = merge.source_table.c["Number"] == ''
            target_placeholder = merge.table.c["Number"] == ''

            merge.exec(
                # source_condition — это WHERE по вьюшке ZakazyKlientovGrouped_view. Фильтруем по
                # ключу группы, т.к. merged_on во вьюшке агрегирован и фильтровать по нему «строки
                # источника» нельзя — у группы другой ключ.
                source_condition=or_(source_key.in_(changed_groups), source_placeholder),
                # delete_condition: чистим все строки изменившихся групп в целевой таблице, затем
                # merge вставит актуальный состав. Удаляем по ключу группы (а не по PK), чтобы
                # убрать и те Artikul, что из группы выпали (в том числе при переименовании Code).
                # Заглушка в scope всегда: строки, ушедшие из неё к своему документу, удалятся
                # именно здесь — во временной таблице их уже нет.
                delete_condition=or_(
                    target_key.in_(select(merge.temp_table.c["Number"], merge.temp_table.c["Year"])),
                    target_placeholder),
            )

        # Почему CHANGED_GROUPS собран через UNION ALL, а не одним WHERE с тремя OR. Вариант с OR
        # читается лучше, но на объёме это узкое место, и дело НЕ в отсутствии индексов. Ветки
        # такого OR живут на разных таблицах соединения, поэтому ни по одной нельзя отфильтровать
        # до джойна: условие проверяется только на готовой паре и вырождается в Join Filter поверх
        # ПОЛНОГО соединения. BitmapOr из индексных сканов Postgres собирает охотно, но только
        # когда все ветки OR на одной таблице, а преобразовывать OR в UNION через границу джойна он
        # не умеет. Тип соединения ни при чём: замена LEFT на INNER плана не меняет.
        #
        # В варианте с UNION ALL в каждой ветке остаётся предикат ровно по ОДНОЙ базовой таблице.
        # Такой предикат планировщик опускает внутрь вьюшки, в скан этой таблицы, и ветка заходит
        # через её индекс merged_on (их заводит сам репликатор, см. _ensure_merged_on_index).
        # LEFT JOIN не мешает: `n.merged_on > since` для несопоставленных строк даёт NULL,
        # предикат null-rejecting, и внешнее соединение в этой ветке схлопывается во внутреннее.
        #
        # Замер на синтетике (регистр 2 млн строк, окно ~0.5%): OR — 620 мс с полным сканом
        # регистра, UNION ALL из той же вьюшки — 98 мс без единого seq scan. Переписывать соединения
        # руками ради этого не нужно: руками получилось 83 мс, разница в пределах накладных
        # расходов на вьюшку, а логика соединений при этом осталась бы в трёх копиях.
        #
        # Ещё два условия, без которых выигрыша не будет:
        #  - random_page_cost под SSD (1.1 вместо дефолтных 4): иначе планировщик предпочтёт seq
        #    scan регистра nested loop'у по индексу, и UNION ALL даст лишь ~2x вместо ~6x;
        #  - индекс по колонке соединения со справочником (Nomenklatura_Key) — см. DDL.
        ctx.logger.info("Витрина обновлена за окно (%s, %s]", self.since(ctx), ctx.boundary)
