"""
Витрина: строки регистра «Заказы клиентов» с артикулом из справочника номенклатуры.

Обработчик зовут сам, когда в БД реально изменился регистр или номенклатура, и передают
окно (context.last_run_at, context.boundary] — выбирать данные обработчик должен по нему.

Ключевая мысль всей конструкции: объекты 1С приезжают в обмене независимо, номенклатура может
доехать позже регистра. Поэтому вьюшка отдаёт merged_on КАЖДОГО источника отдельной колонкой, и в
пересчёт попадает строка, у которой стал свежее хоть один из них. Если ориентироваться только на
merged_on регистра, строка, дождавшаяся своей номенклатуры, в инкремент уже не попадёт и навсегда
останется с NULL-артикулом.
"""

from sqlalchemy import and_, select, tuple_, union_all
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
	s."Period",
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
	"Period" timestamp,
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
-- существовал ради SELECT max(...) из самой витрины, а границу теперь приносит context.
CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientov_merged_on" ON
	{schema}."ZakazyKlientov" USING btree (merged_on);

-- Индекс по колонке соединения со справочником: без него ветка UNION ALL, которую ведёт свежая
-- номенклатура, дотягивается до регистра полным сканом. Репликатор такие индексы не создаёт — он
-- заводит только merged_on, а что с чем соединяется, знает витрина, а не он.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Nomenklatura_Key" ON
	{schema}."AccumulationRegister_ZakazyKlientov" USING btree ("Nomenklatura_Key");

-- Индекс по периоду: по нему нарезана пересборка (см. rebuild ниже), блок = месяц.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Period" ON
	{schema}."AccumulationRegister_ZakazyKlientov" USING btree ("Period");
"""


# Месяцы, за которые в регистре есть движения, по возрастанию. Порядок важен: метка последнего
# блока — это точка возобновления, и «всё, что меньше» должно быть уже сделано. Метка в формате
# YYYY-MM именно поэтому: она сравнивается как строка, и такой формат сортируется правильно сам.
#
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


class ZakazyKlientov(Handler1C):
    # Имена ТАБЛИЦ в целевой БД (транслит), а не имена объектов 1С — те же, что стоят в SQL ниже:
    #   AccumulationRegister_ZakazyKlientov  ← РегистрНакопления.ЗаказыКлиентов
    #   Catalog_Nomenklatura                 ← Справочник.Номенклатура
    # Читаются обе, поэтому обе и перечислены: по этому же списку считается верхняя граница окна
    # (незавершённый merge любой из них прижимает границу к своему старту).
    ON = ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"]

    def setup(self, context):
        # Один раз за процесс, а не на каждый вызов: полная выгрузка сигналит постранично, и
        # CREATE OR REPLACE VIEW на каждую страницу брал бы блокировки на пустом месте.
        self.execute(context, DDL)

    def merge(self, context):
        """Один и тот же merge для инкремента и для блока пересборки — различаются они только
        условиями, которые передаются в exec()."""
        return dbmerge(context.engine, table_name="ZakazyKlientov", schema=context.schema,
                       source_table_name="ZakazyKlientov_view", source_schema=context.schema,
                       delete_mode='delete')

    def rebuild(self, context):
        """
        Пересборка ПО МЕСЯЦАМ. Между блоками цикл применяет накопившиеся изменения, поэтому витрина
        не стоит холодной все те десятки минут, что идёт пересборка (см. Handler1C.rebuild).

        Блок читает данные АКТУАЛЬНЫЕ на момент своего выполнения — никакого снимка на старте
        пересборки. Иначе блок затёр бы изменения, применённые между блоками.
        """
        for label, begin, end in self.query(context, REBUILD_BLOCKS_SQL):
            if context.rebuild_from and label <= context.rebuild_from:
                continue                      # этот месяц уже посчитан до перезапуска процесса

            with self.merge(context) as merge:
                target_period = merge.table.c["Period"]
                period = merge.source_table.c["Period"]
                # Блок берёт из источника ВЕСЬ месяц, поэтому и удалять можно весь месяц: что не
                # приехало в staging, того в источнике больше нет. Инкременту так нельзя — он
                # видит лишь часть строк месяца и снёс бы остальные.
                #
                # Заодно это чинит переезд строки между месяцами (поправили дату документа): из
                # старого месяца её удалит его блок, в новом создаст его собственный. Удаляй мы
                # только по регистраторам из staging, в старом месяце осталась бы копия.
                merge.exec(
                    source_condition=and_(period >= begin, period < end),
                    delete_condition=and_(target_period >= begin, target_period < end))

            context.logger.info("Витрина пересобрана за %s", label)
            yield label

    def handle(self, context):
        with self.merge(context) as merge:

            # Единица пересчёта — не строка, а ГРУППА (Recorder), потому что удаляем мы тоже
            # группой. Берём ключи групп, у которых стал свежее хоть один источник.
            #
            # Вьюшка у веток одна и та же: логика соединений не дублируется, различаются они
            # только колонкой в WHERE. ALL — потому что дедупликация не нужна: результат уходит
            # в IN (...), где дубликаты не мешают.
            changed = merge.source_table.alias("chg")
            since = self.since(context)

            changed_by_register = (
                select(changed.c["Recorder"], changed.c["Recorder_Type"])
                .where(changed.c["merged_on"] > since))

            changed_by_nomenklatura = (
                select(changed.c["Recorder"], changed.c["Recorder_Type"])
                .where(changed.c["Nomenklatura_merged_on"] > since))

            changed_recorders = union_all(changed_by_register, changed_by_nomenklatura)

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
            # Почему changed_recorders собран через UNION ALL, а не одним WHERE с OR (то есть не
            # через self.changed_since). Написать `merged_on > since OR Nomenklatura_merged_on >
            # since` — читается лучше, но на объёме это узкое место, и дело НЕ в отсутствии
            # индексов. Ветки такого OR живут на разных таблицах соединения, поэтому ни по одной
            # из них нельзя отфильтровать до джойна: условие проверяется только на готовой паре и
            # вырождается в Join Filter поверх ПОЛНОГО соединения. BitmapOr из индексных сканов
            # Postgres собирает охотно, но только когда все ветки OR на одной таблице, а
            # преобразовывать OR в UNION через границу джойна он не умеет. Тип соединения тут ни
            # при чём: замена LEFT на INNER плана не меняет.
            #
            # В варианте с UNION ALL в каждой ветке остаётся предикат ровно по ОДНОЙ базовой
            # таблице. Такой предикат планировщик опускает внутрь вьюшки, в скан этой таблицы, и
            # ветка заходит через её индекс merged_on (их заводит сам репликатор, см.
            # DBWriter1C._ensure_merged_on_index). LEFT JOIN не мешает: `n.merged_on > since` для
            # несопоставленных строк даёт NULL, предикат null-rejecting, и внешнее соединение в
            # этой ветке схлопывается во внутреннее.
            #
            # Замер на синтетике (регистр 2 млн строк, окно ~0.5%): OR — 620 мс с полным сканом
            # регистра, UNION ALL из той же вьюшки — 98 мс без единого seq scan. Переписывать
            # соединения руками ради этого не нужно: руками получилось 83 мс, разница в пределах
            # накладных расходов на вьюшку.
            #
            # Ещё два условия, без которых выигрыша не будет:
            #  - random_page_cost под SSD (1.1 вместо дефолтных 4): иначе планировщик предпочтёт
            #    seq scan регистра nested loop'у по индексу, и UNION ALL даст лишь ~2x вместо ~6x;
            #  - индекс по колонке соединения со справочником (Nomenklatura_Key) — репликатор
            #    заводит только merged_on, поэтому этот индекс создаётся в setup().

        context.logger.info("Витрина обновлена за окно (%s, %s]",
                            self.since(context), context.boundary)
