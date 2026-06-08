import logging

from sqlalchemy import Engine, tuple_
from dbmerge import dbmerge

from cdc_1C.DataReader1C import DataReader1C, DataObject1C
from cdc_1C.NameMapper1C import NameMapper1C

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DBWriter1C:
    """
    Сохраняет объекты 1С (DataObject1C) в БД через dbmerge.
    Имена таблиц и колонок переводятся NameMapper1C, типы и первичный ключ берутся из метаданных.
    Обрабатывает объекты по одному (см. save_all).
    """

    def __init__(self, engine: Engine, name_mapper: NameMapper1C,
                 delete_mode: str = 'no', schema: str | None = None):
        self.engine = engine
        self.name_mapper = name_mapper
        self.delete_mode = delete_mode
        self.schema = schema

    def save(self, object_name: str, data_object: DataObject1C) -> None:
        if data_object.data_length == 0:
            return

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

        delete_key = metadata_obj.delete_key

        if not delete_key:
            # Документ/справочник: одна запись на Ref_Key, удалять чужие строки не нужно.
            with dbmerge(engine=self.engine, table_name=table_name, data=records,
                         key=key, data_types=data_types,
                         delete_mode=self.delete_mode, schema=self.schema) as merge:
                merge.exec()
        else:
            # Регистр/табличная часть: набор по delete_key целиком заменяет существующий.
            # Удаляем строки только тех групп, что пришли в наборе, и которых больше нет в источнике.
            mapped_delete_key = [self.name_mapper.map_field_name(k) for k in delete_key]
            with dbmerge(engine=self.engine, table_name=table_name, data=records,
                         key=key, data_types=data_types,
                         delete_mode='delete', schema=self.schema) as merge:
                condition = self._scoped_delete_condition(merge.table, mapped_delete_key, records)
                merge.exec(delete_condition=condition)

    @staticmethod
    def _scoped_delete_condition(table, mapped_delete_key: list[str], records: list[dict]):
        """
        Ограничивает удаление строками тех групп (по delete_key), что присутствуют в наборе.
        Какие именно строки удалить (отсутствующие в источнике), dbmerge определяет по PK.

        Один столбец (Ref_Key табличной части):  Ref_Key IN (...)
        Несколько (Recorder + Recorder_Type):     (Recorder, Recorder_Type) IN ((..,..), ...)
        """
        if len(mapped_delete_key) == 1:
            col = mapped_delete_key[0]
            values = {record[col] for record in records}
            return table.c[col].in_(values)

        values = {tuple(record[col] for col in mapped_delete_key) for record in records}
        return tuple_(*[table.c[col] for col in mapped_delete_key]).in_(values)

    def save_all(self, data_reader: DataReader1C) -> None:
        """
        Сохраняет все объекты по одному. Каждый объект мержится отдельным вызовом dbmerge
        (своя транзакция). Исключение пробрасывается, чтобы не подтверждать получение изменений
        при неполном сохранении.
        """
        for object_name, data_object in data_reader.items():
            self.save(object_name, data_object)
