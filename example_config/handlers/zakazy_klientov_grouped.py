"""
Витрина с ДРУГИМ ключом: агрегат по (Номер документа, Год, Артикул), а не по строкам регистра.
Второй пример: как та же схема работает, когда ключ витрины не совпадает с ключом источника.

Ключ группы здесь составной — (Number, Year). Это меняет две вещи: запрос «что пересобрать» отдаёт
две колонки, а фильтры собираются row-value сравнением `tuple_(a, b).in_(...)`.

Главное же отличие в том, что ключ ВЫЧИСЛЯЕТСЯ из данных: Number берётся из документа, Year — из его
даты. Значит, изменение в 1С может не изменить строку, а перенести её в другую группу: доехал
документ — строки ушли из группы-заглушки в реальную; поправили дату через границу года — ушли из
(N, 2025) в (N, 2026). Новую группу видно в источнике, а старой нет уже нигде, и удалить её нечем.

Поэтому витрина хранит колонки-массивы "*_Keys": GUID-ы объектов 1С, из которых собрана строка.
Обработчик берёт GUID-ы изменившихся объектов и получает по ним два списка групп — что эти объекты
давали раньше (по массивам витрины) и что дают сейчас (по построчному слою), — после чего
пересобирает оба. Покинутая группа при этом чистится сама.

Смена ключа требует двух слоёв вьюшек:
  1) _rows_view — построчный: все JOIN'ы описаны один раз, каждый источник отдаёт свой merged_on
     отдельной колонкой, ссылки на источники выходят наружу как есть;
  2) _view — агрегат поверх него, он и есть источник merge; здесь же ссылки сворачиваются в массивы.

Соединения — LEFT, потому что объекты 1С приезжают в обмене независимо: движения регистра могут
доехать раньше своего документа или номенклатуры. INNER JOIN спрятал бы такие строки до прибытия
документа; LEFT показывает их сразу, ценой NULL в ключе группы — отсюда COALESCE ниже.

Пересборка нарезана по месяцам периода регистра (см. rebuild), и та же вычисляемость ключа делает
блок не таким прямолинейным, как хотелось бы. Взять «строки месяца» нельзя: группа собирается из
строк, и месяц отрезал бы от неё часть, а удаляем мы группой. Поэтому блок отбирает ГРУППЫ — и ровно
тем же PREVIOUS/CURRENT, что и инкремент: что регистраторы этого месяца давали раньше (по массивам
"*_Keys" витрины) и что дают сейчас. Без половины PREVIOUS пересборка была бы неполной: группу,
которую покинули ещё до её начала, не нашёл бы ни один блок.

Второе следствие проявляется уже во время самой пересборки: она идёт минутами, и за это время
документ может переехать в группу, чей блок давно позади. Убирает покинутую группу ИНКРЕМЕНТ,
который цикл выполняет между блоками, — тем же механизмом и без всякой доработки.
"""

from sqlalchemy import text, tuple_

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
	n."merged_on" "Nomenklatura_merged_on",
	--Период движения: по нему нарезана пересборка (блок = месяц). В агрегат он не идёт — в ключе
	--витрины его нет, он нужен только чтобы отобрать группы блока.
	s."Period",
	--Ссылки на источники: ниже они сворачиваются в массивы, по которым ищутся покинутые группы.
	--Регистратор и документ — один и тот же GUID, поэтому колонка одна на оба источника.
	s."Recorder",
	s."Nomenklatura_Key"
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
	MAX("merged_on") "merged_on",
	--Массивы собираются по всей группе, а не по одной строке: если строку удалят, группа должна
	--достаться остальным. array_agg(DISTINCT ...) сортирует, поэтому массив стабилен и не даёт
	--ложных UPDATE. FILTER отсекает NULL, COALESCE подставляет пустой массив.
	COALESCE(array_agg(DISTINCT "Recorder") FILTER (WHERE "Recorder" IS NOT NULL),
		ARRAY[]::uuid[]) "Recorder_Keys",
	COALESCE(array_agg(DISTINCT "Nomenklatura_Key") FILTER (WHERE "Nomenklatura_Key" IS NOT NULL),
		ARRAY[]::uuid[]) "Nomenklatura_Keys"
FROM {schema}."ZakazyKlientovGrouped_rows_view"
GROUP BY "Number","Year","Artikul"
);

CREATE TABLE IF NOT EXISTS {schema}."ZakazyKlientovGrouped" (
	"Number" varchar,
	"Year" int4,
	"Artikul" varchar,
	"Zakazano" numeric,
	"merged_on" timestamp,
	"Recorder_Keys" uuid[],
	"Nomenklatura_Keys" uuid[],
	CONSTRAINT "ZakazyKlientovGrouped_pkey"
		PRIMARY KEY ("Number", "Year", "Artikul")
);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_merged_on" ON
	{schema}."ZakazyKlientovGrouped" USING btree (merged_on);

-- GIN на каждый массив: под запрос «какие группы породил вот этот набор GUID-ов» (оператор &&).
-- Btree тут не работает: ищем не по значению массива, а по вхождению элемента.
CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_Recorder_Keys" ON
	{schema}."ZakazyKlientovGrouped" USING gin ("Recorder_Keys");

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_Nomenklatura_Keys" ON
	{schema}."ZakazyKlientovGrouped" USING gin ("Nomenklatura_Keys");

-- Индекс под source_condition: агрегатная вьюшка фильтруется по ключу группы, а Number приходит
-- из документа.
CREATE INDEX IF NOT EXISTS "ix_Document_ZakazKlienta_Number" ON
	{schema}."Document_ZakazKlienta" USING btree ("Number");

-- Индекс по колонке соединения со справочником. Репликатор такие индексы не создаёт — он заводит
-- только merged_on, а что с чем соединяется, знает витрина, а не он.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Nomenklatura_Key" ON
	{schema}."AccumulationRegister_ZakazyKlientov" USING btree ("Nomenklatura_Key");

-- Индекс по периоду: по нему нарезана пересборка (см. rebuild ниже), блок = месяц.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Period" ON
	{schema}."AccumulationRegister_ZakazyKlientov" USING btree ("Period");
"""

# Группы, которые надо пересобрать за окно (:since — конец прошлого прогона):
# PREVIOUS — что изменившиеся объекты давали раньше, CURRENT — что дают сейчас.
#
# Две тонкости, из-за которых половины записаны по-разному:
#   - в PREVIOUS оба условия на одной таблице, там OR работает через индексы; в CURRENT они на
#     разных таблицах соединения, и OR пришлось бы проверять уже после JOIN'а — поэтому UNION ALL;
#   - набор изменившегося подставляется через ARRAY(SELECT ...), а не IN (SELECT ...): под OR
#     IN-подзапрос не позволяет планировщику использовать индексы.
GROUPS_TO_REBUILD_SQL = """
WITH changed_recorders AS (
    SELECT DISTINCT "Recorder" AS id
      FROM {schema}."AccumulationRegister_ZakazyKlientov"
     WHERE "merged_on" > :since
    UNION
    SELECT "Ref_Key" FROM {schema}."Document_ZakazKlienta"
     WHERE "merged_on" > :since
),
changed_nomenklatura AS (
    SELECT "Ref_Key" AS id FROM {schema}."Catalog_Nomenklatura"
     WHERE "merged_on" > :since
)

-- PREVIOUS: группы, которые изменившиеся объекты давали раньше
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped"
 WHERE "Recorder_Keys" && ARRAY(SELECT id FROM changed_recorders)
    OR "Nomenklatura_Keys" && ARRAY(SELECT id FROM changed_nomenklatura)

UNION ALL

-- CURRENT: группы, которые они дают сейчас
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped_rows_view"
 WHERE "merged_on" > :since
UNION ALL
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped_rows_view"
 WHERE "ZakazKlienta_merged_on" > :since
UNION ALL
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped_rows_view"
 WHERE "Nomenklatura_merged_on" > :since
"""


# Месяцы, за которые в регистре есть движения. Нарезаем по периоду РЕГИСТРА, а не по дате
# документа: период есть у любой строки, включая те, чей документ ещё не доехал (у них Year=0 и
# пустой Number — в нарезку по дате документа они бы просто не попали).
#
# Метка YYYY-MM сравнивается как строка, и такой формат сортируется правильно сам по себе.
# Арифметику календаря делает БД: прибавить месяц к дате в Python — либо лишняя зависимость, либо
# трюк вроде «28-е плюс четыре дня».
REBUILD_BLOCKS_SQL = """
SELECT DISTINCT to_char(date_trunc('month', "Period"), 'YYYY-MM') AS label,
       date_trunc('month', "Period")::date AS begins,
       (date_trunc('month', "Period") + interval '1 month')::date AS ends
  FROM {schema}."AccumulationRegister_ZakazyKlientov"
 WHERE "Period" IS NOT NULL
 ORDER BY label
"""

# Группы одного блока пересборки. Форма та же, что у GROUPS_TO_REBUILD_SQL, — меняется только
# способ отбора: там «что изменилось за окно», здесь «что лежит в этом месяце».
#
# PREVIOUS нужен по той же причине, что и в инкременте, и без него пересборка была бы неполной.
# Ключ группы ВЫЧИСЛЯЕТСЯ из даты документа, поэтому группа умеет переезжать: правка даты через
# границу года переносит строки из (N, 2025) в (N, 2026). Новую группу видно в источнике, старой
# нет уже нигде — и удалить её можно только через массивы "*_Keys" витрины.
GROUPS_IN_BLOCK_SQL = """
WITH block_recorders AS (
    SELECT DISTINCT "Recorder" AS id
      FROM {schema}."AccumulationRegister_ZakazyKlientov"
     WHERE "Period" >= :begins AND "Period" < :ends
)

-- PREVIOUS: что регистраторы этого месяца давали раньше
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped"
 WHERE "Recorder_Keys" && ARRAY(SELECT id FROM block_recorders)

UNION ALL

-- CURRENT: что они дают сейчас
SELECT "Number", "Year" FROM {schema}."ZakazyKlientovGrouped_rows_view"
 WHERE "Period" >= :begins AND "Period" < :ends
"""


class ZakazyKlientovGrouped(Handler1C):
    # Имена ТАБЛИЦ в целевой БД (транслит), а не имена объектов 1С — те же, что стоят в SQL выше:
    #   AccumulationRegister_ZakazyKlientov  ← РегистрНакопления.ЗаказыКлиентов
    #   Document_ZakazKlienta                ← Документ.ЗаказКлиента
    #   Catalog_Nomenklatura                 ← Справочник.Номенклатура
    # Читаются все три, поэтому все три и перечислены: по этому же списку считается верхняя
    # граница окна.
    ON = ["AccumulationRegister_ZakazyKlientov", "Document_ZakazKlienta", "Catalog_Nomenklatura"]

    def setup(self, context):
        self.execute(context, DDL)

    def merge(self, context):
        """Один и тот же merge для инкремента и для блока пересборки — различаются они только
        условиями, которые уходят в exec()."""
        return dbmerge(context.engine, table_name="ZakazyKlientovGrouped", schema=context.schema,
                       source_table_name="ZakazyKlientovGrouped_view", source_schema=context.schema,
                       delete_mode='delete')

    def rebuild(self, context):
        """
        Пересборка ПО МЕСЯЦАМ. Между блоками цикл применяет накопившиеся изменения, поэтому витрина
        не стоит холодной все те десятки минут, что идёт пересборка (см. Handler1C.rebuild).

        Единица блока — не строка витрины, а ГРУППА, как и в инкременте: удаляем мы группой, значит
        и пересобирать обязаны группой целиком. Взять «строки месяца» нельзя — группа собирается из
        строк, и месяц отрезал бы от неё часть.
        """
        for label, begins, ends in self.query(context, REBUILD_BLOCKS_SQL):
            if context.rebuild_from and label <= context.rebuild_from:
                continue                  # этот месяц уже посчитан до перезапуска процесса

            groups = text(GROUPS_IN_BLOCK_SQL.format(schema=self.schema_prefix(context))
                          ).bindparams(begins=begins, ends=ends)

            with self.merge(context) as merge:
                source_key = tuple_(merge.source_table.c["Number"], merge.source_table.c["Year"])
                target_key = tuple_(merge.table.c["Number"], merge.table.c["Year"])
                merge.exec(source_condition=source_key.in_(groups),
                           delete_condition=target_key.in_(groups))

            context.logger.info("Data Mart updated for %s", label)
            yield label

    def handle(self, context):
        groups_to_rebuild = text(
            GROUPS_TO_REBUILD_SQL.format(schema=self.schema_prefix(context))
        ).bindparams(since=self.since(context))

        with self.merge(context) as merge:
            # Ключ группы составной, поэтому сравнение row-value: (Number, Year) IN (SELECT
            # Number, Year ...). В остальном ничего не меняется по сравнению с одиночным ключом.
            source_key = tuple_(merge.source_table.c["Number"], merge.source_table.c["Year"])
            target_key = tuple_(merge.table.c["Number"], merge.table.c["Year"])

            # Берём из вьюшки все строки этих групп и удаляем в витрине тоже по ним, а не по
            # содержимому staging-таблицы. Группы, из которой всё уехало, в staging нет — по нему
            # её было бы не удалить. Удаление по ключу ГРУППЫ (а не по PK) заодно убирает те
            # Artikul, что из группы выпали, в том числе при переименовании Code.
            merge.exec(source_condition=source_key.in_(groups_to_rebuild),
                       delete_condition=target_key.in_(groups_to_rebuild))

        context.logger.info(
            f'Data Mart changes applied since {self.since(context)}; '
            f'Next run will apply since {context.boundary}')