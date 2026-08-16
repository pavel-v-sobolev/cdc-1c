"""
Витрина: строки регистра «Заказы клиентов» с артикулом из справочника номенклатуры.

Раннер зовёт обработчик сам, когда в БД реально изменился регистр или номенклатура, и передаёт
окно (ctx.last_run_at, ctx.boundary] — выбирать данные обработчик должен по нему.

Ключевая мысль всей конструкции: объекты 1С приезжают в обмене независимо, номенклатура может
доехать позже регистра. Поэтому вьюшка отдаёт merged_on КАЖДОГО источника отдельной колонкой, и в
пересчёт попадает строка, у которой стал свежее хоть один из них. Если ориентироваться только на
merged_on регистра, строка, дождавшаяся своей номенклатуры, в инкремент уже не попадёт и навсегда
останется с NULL-артикулом.
"""

from sqlalchemy import select, tuple_, union_all
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

-- Индекс по колонке соединения со справочником: без него ветка UNION ALL, которую ведёт свежая
-- номенклатура, дотягивается до регистра полным сканом. Репликатор такие индексы не создаёт — он
-- заводит только merged_on, а что с чем соединяется, знает витрина, а не он.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Nomenklatura_Key" ON
	{schema}."AccumulationRegister_ZakazyKlientov" USING btree ("Nomenklatura_Key");
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
            # группой. Берём ключи групп, у которых стал свежее хоть один источник.
            #
            # Не OR по всем отметкам сразу, а UNION ALL по одной отметке на ветку — почему так,
            # см. длинный комментарий в конце метода. Вьюшка при этом одна и та же: логика
            # соединений не дублируется, ветки различаются только колонкой в WHERE. ALL — потому
            # что дедупликация не нужна: результат уходит в IN (...), где дубликаты не мешают.
            changed = merge.source_table.alias("chg")
            since = self.since(ctx)
            changed_recorders = union_all(*[
                select(changed.c["Recorder"], changed.c["Recorder_Type"]).where(changed.c[column] > since)
                for column in ("merged_on", "Nomenklatura_merged_on")
            ])

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

        ctx.logger.info("Витрина обновлена за окно (%s, %s]", self.since(ctx), ctx.boundary)
