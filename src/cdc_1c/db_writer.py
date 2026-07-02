import logging

from sqlalchemy import Engine, Index, MetaData, Table, tuple_, select, or_, and_, exists
from dbmerge import dbmerge, mergeResult

from cdc_1c.data_reader import DataObject1C, EXCHANGE_MESSAGE_NO_FIELD
from cdc_1c.name_mapper import NameMapper1C

logger = logging.getLogger(__name__)

# Служебные поля, которыми управляет dbmerge (момент merge/первой вставки строки).
MERGED_ON_FIELD = 'merged_on'
INSERTED_ON_FIELD = 'inserted_on'

# Порядок сохранения объектов: справочники → документы → регистры. Документы ссылаются на
# справочники (по *_Key), регистры — на документы (Recorder), поэтому родителей сохраняем раньше.
# Табличные части (Catalog_X_Y / Document_X_Y) попадают в группу своего владельца по префиксу.
SAVE_ORDER_PREFIXES = ('Catalog', 'Document', 'InformationRegister', 'AccumulationRegister')


def save_order_key(object_name: str) -> int:
    """Приоритет объекта в порядке сохранения (см. SAVE_ORDER_PREFIXES); неизвестные типы — в конец."""
    for i, prefix in enumerate(SAVE_ORDER_PREFIXES):
        if object_name.startswith(prefix):
            return i
    return len(SAVE_ORDER_PREFIXES)


class DBWriter1C:
    """
    Сохраняет объекты 1С (DataObject1C) в БД через dbmerge, по одному вызовом save().
    Имена таблиц и колонок переводятся NameMapper1C, типы и первичный ключ берутся из метаданных.

    Заполняет служебные merged_on/inserted_on и создаёт индекс по merged_on (для инкрементальной
    материализации). Лог загрузки (replicator_1c_log) пишет оркестратор Replicator1C — у writer-а нет
    контекста обмена (его можно использовать и для полной перевыгрузки через read_object, где нет
    номера пакета).

    Версионирование (см. save, full_load): у каждой записи есть exchange_message_no (emn) — номер
    пакета обмена у изменений, 0 у полной выгрузки. Полная выгрузка (full_load=True) не перезаписывает
    более свежие данные из потока изменений (guard'ы по emn); изменения авторитетны и идут в порядке
    пакетов, поэтому guard'ами не ограничиваются.
    """

    def __init__(self, engine: Engine, name_mapper: NameMapper1C, schema: str | None = None):
        self.engine = engine
        self.name_mapper = name_mapper
        self.schema = schema
        # Таблицы, для которых индекс по merged_on уже обеспечен в этом процессе (чтобы не рефлексить
        # и не дёргать checkfirst на каждом save).
        self._indexed_tables: set[str] = set()

    def save(self, object_name: str, data_object: DataObject1C, full_load: bool = False) -> mergeResult | None:
        """
        Сохраняет один объект через dbmerge.

        Режим изменений (full_load=False, по умолчанию): для регистров/табличных частей — scoped-
        удаление по object_key (набор группы заменяется целиком), для документов/справочников —
        чистый upsert по ключу. Изменения авторитетны, поэтому version-guard'ами не ограничиваются.

        Режим полной выгрузки (full_load=True): записи штампуются emn=0 (см. full_load), и применяются
        version-guard'ы, чтобы устаревший снимок не затирал более свежие изменения (emn>0):
        - документ/справочник: upsert без удаления + update_condition (перезаписываем только строки
          без версии/со своей версией; удаление у документов мягкое — строка остаётся, это update);
        - регистр/табличная часть: own-or-skip группы целиком (группа умещается на одной странице) —
          scoped-удаление с version-guard'ом, update_condition и insert_condition, чтобы «горячую»
          группу (есть строка с emn>0) не трогать, а «свою» (emn NULL/0) заменить снимком.

        Возвращает mergeResult, либо None на ранних выходах (пустой набор / нет метаданных) —
        лог загрузки принимает None (write_result тогда просто не прибавляет счётчики).
        """
        if data_object.data_length == 0:
            return None

        logger.info(f"Saving {data_object.data_length} records of {object_name}")
        metadata_obj = data_object.metadata_obj
        if metadata_obj is None or not metadata_obj.primary_key:
            logger.warning(f'No metadata/primary key for {object_name}, skipping save')
            return None

        table_name = self.name_mapper.map_object_name(object_name)

        col_map = self.name_mapper.get_column_mapping(list(data_object.data.keys()))
        records = data_object.to_records_mapped(col_map)

        key = [self.name_mapper.map_field_name(k) for k in metadata_obj.primary_key]
        data_types = {self.name_mapper.map_field_name(col): typ
                      for col, typ in metadata_obj.get_column_types().items()}

        object_key = metadata_obj.object_key
        emn = self.name_mapper.map_field_name(EXCHANGE_MESSAGE_NO_FIELD)

        if not object_key:
            # Документ/справочник (одна запись по ключу): чистый upsert без удаления.
            with dbmerge(engine=self.engine, table_name=table_name, data=records,
                         key=key, data_types=data_types,
                         merged_on_field=MERGED_ON_FIELD, inserted_on_field=INSERTED_ON_FIELD,
                         delete_mode='no', schema=self.schema) as merge:
                result = merge.exec(
                    update_condition=self._version_guard(merge, emn) if full_load else None)
        else:
            # Регистр/табличная часть: набор по object_key целиком заменяет существующий.
            # Удаляем строки только тех групп, что пришли в наборе, и которых больше нет в источнике.
            mapped_object_key = [self.name_mapper.map_field_name(k) for k in object_key]
            with dbmerge(engine=self.engine, table_name=table_name, data=records,
                         key=key, data_types=data_types,
                         merged_on_field=MERGED_ON_FIELD, inserted_on_field=INSERTED_ON_FIELD,
                         delete_mode='delete', schema=self.schema) as merge:
                scoped = self._scoped_delete_condition(merge.table, merge.temp_table, mapped_object_key)
                if full_load:
                    # own-or-skip группы: не трогаем группы с более свежей версией (emn>0).
                    result = merge.exec(
                        delete_condition=and_(scoped, self._not_newer(merge, emn)),
                        update_condition=self._version_guard(merge, emn),
                        insert_condition=self._group_not_newer(merge, mapped_object_key, emn))
                else:
                    result = merge.exec(delete_condition=scoped)

        self._ensure_merged_on_index(table_name)
        return result

    @staticmethod
    def _version_guard(merge, emn: str):
        """update только если целевая строка пустая по версии или не новее входящей (emn)."""
        return or_(merge.table.c[emn].is_(None), merge.temp_table.c[emn] >= merge.table.c[emn])

    @staticmethod
    def _not_newer(merge, emn: str):
        """строку можно удалить полной выгрузкой, только если она не новее снимка (emn NULL или <=0)."""
        return or_(merge.table.c[emn].is_(None), merge.table.c[emn] <= 0)

    @staticmethod
    def _group_not_newer(merge, mapped_object_key: list[str], emn: str):
        """
        Не вставлять строку в группу (по object_key), где уже есть строка от изменения (emn>0):
        коррелированный NOT EXISTS по отдельному алиасу целевой таблицы (в insert-фазе строка
        целевой таблицы по PK отсутствует, поэтому смотрим на группу через alias).
        """
        g = merge.table.alias()
        conds = [g.c[col] == merge.temp_table.c[col] for col in mapped_object_key]
        conds.append(g.c[emn] > 0)
        return ~exists().where(and_(*conds))

    def _ensure_merged_on_index(self, table_name: str) -> None:
        """
        Индекс по merged_on (для быстрых сканов материализатора `merged_on > граница обработки`).
        Только средствами SQLAlchemy, идемпотентно (checkfirst). Таблицу уже создал dbmerge.
        Результат кэшируется на инстансе — рефлексия/checkfirst выполняются один раз на таблицу.
        """
        if table_name in self._indexed_tables:
            return
        eff_schema = None if self.engine.dialect.name == 'sqlite' else self.schema
        tbl = Table(table_name, MetaData(), schema=eff_schema, autoload_with=self.engine)
        if MERGED_ON_FIELD in tbl.c:
            ix_name = NameMapper1C._fit_length(f'ix_{table_name}_merged_on')
            Index(ix_name, tbl.c[MERGED_ON_FIELD]).create(self.engine, checkfirst=True)
        self._indexed_tables.add(table_name)

    @staticmethod
    def _scoped_delete_condition(table, temp_table, mapped_object_key: list[str]):
        """
        Ограничивает удаление строками тех групп (по object_key), что присутствуют в staging-таблице
        (temp_table). Какие именно строки внутри групп удалить (отсутствующие в источнике),
        dbmerge определяет по PK.

        Внешние колонки — целевой таблицы, значения групп берём подзапросом из temp_table:
          один столбец:   target.Ref_Key IN (SELECT Ref_Key FROM temp)
          несколько:      (target.Recorder, target.Recorder_Type) IN (SELECT Recorder, Recorder_Type FROM temp)
        """
        if len(mapped_object_key) == 1:
            col = mapped_object_key[0]
            return table.c[col].in_(select(temp_table.c[col]))

        target_cols = [table.c[col] for col in mapped_object_key]
        temp_cols = [temp_table.c[col] for col in mapped_object_key]
        return tuple_(*target_cols).in_(select(*temp_cols))
