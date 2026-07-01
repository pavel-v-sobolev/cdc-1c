import logging

from sqlalchemy import Engine, Index, MetaData, Table, tuple_, select
from dbmerge import dbmerge

from cdc_1c.data_reader import DataReader1C, DataObject1C
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
    Сохраняет объекты 1С (DataObject1C) в БД через dbmerge.
    Имена таблиц и колонок переводятся NameMapper1C, типы и первичный ключ берутся из метаданных.
    Обрабатывает объекты по одному (см. save_all).

    Заполняет служебные merged_on/inserted_on и создаёт индекс по merged_on (для инкрементальной
    материализации). Лог загрузки (replicator_1c_log) пишет оркестратор Replicator1C — у writer-а нет
    контекста обмена (его можно использовать и для полной перевыгрузки через read_object, где нет
    номера пакета).
    """

    def __init__(self, engine: Engine, name_mapper: NameMapper1C,
                 data_reader: DataReader1C, schema: str | None = None):
        self.engine = engine
        self.name_mapper = name_mapper
        self.data_reader = data_reader
        self.schema = schema

    def save(self, object_name: str, data_object: DataObject1C, delete: bool = True) -> None:
        """
        Сохраняет один объект через dbmerge. delete=True (по умолчанию, режим изменений): для
        регистров/табличных частей делает scoped-удаление по object_key (набор группы заменяется
        целиком). delete=False — чистый upsert без удаления (для полной постраничной выгрузки, где
        группа может быть разрезана между страницами и удалять отсутствующее на странице нельзя).
        """
        if data_object.data_length == 0:
            return

        logger.info(f"Saving {data_object.data_length} records of {object_name}")
        metadata_obj = data_object.metadata_obj
        if metadata_obj is None or not metadata_obj.primary_key:
            logger.warning(f'No metadata/primary key for {object_name}, skipping save')
            return

        table_name = self.name_mapper.map_object_name(object_name)

        col_map = self.name_mapper.get_column_mapping(list(data_object.data.keys()))
        records = data_object.to_records_mapped(col_map)

        key = [self.name_mapper.map_field_name(k) for k in metadata_obj.primary_key]
        data_types = {self.name_mapper.map_field_name(col): typ
                      for col, typ in metadata_obj.get_column_types().items()}

        object_key = metadata_obj.object_key

        if not object_key or not delete:
            # Документ/справочник (или полная выгрузка): чистый upsert по ключу, без удаления.
            with dbmerge(engine=self.engine, table_name=table_name, data=records,
                         key=key, data_types=data_types,
                         merged_on_field=MERGED_ON_FIELD, inserted_on_field=INSERTED_ON_FIELD,
                         delete_mode='no', schema=self.schema) as merge:
                result = merge.exec()
        else:
            # Регистр/табличная часть: набор по object_key целиком заменяет существующий.
            # Удаляем строки только тех групп, что пришли в наборе, и которых больше нет в источнике.
            mapped_object_key = [self.name_mapper.map_field_name(k) for k in object_key]
            with dbmerge(engine=self.engine, table_name=table_name, data=records,
                         key=key, data_types=data_types,
                         merged_on_field=MERGED_ON_FIELD, inserted_on_field=INSERTED_ON_FIELD,
                         delete_mode='delete', schema=self.schema) as merge:
                condition = self._scoped_delete_condition(merge.table, merge.temp_table, mapped_object_key)
                result = merge.exec(delete_condition=condition)

        self._ensure_merged_on_index(table_name)
        return result

    def _ensure_merged_on_index(self, table_name: str) -> None:
        """
        Индекс по merged_on (для быстрых сканов материализатора `merged_on > граница обработки`).
        Только средствами SQLAlchemy, идемпотентно (checkfirst). Таблицу уже создал dbmerge.
        """
        eff_schema = None if self.engine.dialect.name == 'sqlite' else self.schema
        tbl = Table(table_name, MetaData(), schema=eff_schema, autoload_with=self.engine)
        if MERGED_ON_FIELD not in tbl.c:
            return
        ix_name = NameMapper1C._fit_length(f'ix_{table_name}_merged_on')
        Index(ix_name, tbl.c[MERGED_ON_FIELD]).create(self.engine, checkfirst=True)

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

    def save_all(self) -> None:
        """
        Сохраняет все объекты data_reader по одному. Каждый объект мержится отдельным вызовом
        dbmerge (своя транзакция). Исключение пробрасывается, чтобы не подтверждать получение
        изменений при неполном сохранении.

        Порядок сохранения — справочники → документы → регистры (save_order_key); внутри группы
        исходный порядок (сортировка стабильна).
        """
        for object_name, data_object in sorted(self.data_reader.items(),
                                               key=lambda kv: save_order_key(kv[0])):
            self.save(object_name, data_object)
