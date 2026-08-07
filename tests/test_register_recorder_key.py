"""
Регистраторный регистр, у которого регистратором может быть только один тип документа: 1С отдаёт
поле регистратора как Recorder_Key (Edm.Guid) и не отдаёт Recorder_Type — в отличие от составного
регистратора (Recorder + Recorder_Type).

Проверяем, что такой регистр разбирается как регистраторный (в т.ч. пустой набор = удалённый),
а не как независимый регистр сведений, и что поля ключа не уезжают в NULL.
"""

import uuid
from datetime import datetime

import pytest

from cdc_1c.data_reader import (DataReader1C, EMPTY_UUID, EXCHANGE_MESSAGE_NO_FIELD,
                                IS_DELETED_OR_EMPTY_FIELD)
from cdc_1c.metadata_reader import MetadataObject1C, MetadataReader1C

REG = "InformationRegister_Forecast"
REC = "410d24b4-8774-11f1-abae-6cb3117b9496"

# Поля регистра: ключ — регистратор + период + измерение-ссылка; PlannedQuantity вне ключа.
_FIELDS = {"Recorder_Key": "Guid", "Period": "DateTime", "Customer_Key": "Guid",
           "LineNumber": "Int64", "PlannedQuantity": "Int64"}
_PRIMARY_KEY = {"Recorder_Key": "Guid", "Period": "DateTime", "Customer_Key": "Guid"}


def _reader(properties: dict, primary_key: dict | None = None,
            object_key: list[str] | None = None) -> DataReader1C:
    """DataReader1C с подставленными метаданными регистра (без сети)."""
    metadata = MetadataReader1C(odata_url="http://fake")
    metadata[REG] = MetadataObject1C(REG, dict(properties), dict(primary_key or _PRIMARY_KEY),
                                     object_key=object_key)
    metadata.is_loaded = True
    reader = DataReader1C(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 7
    return reader


def _row(**overrides) -> dict:
    row = {"d:Recorder_Key": REC, "d:Period": "2026-01-01T00:00:05",
           "d:Customer_Key": "5e51e8e9-6821-11ec-a232-00155de3390c",
           "d:LineNumber": "1", "d:PlannedQuantity": "50"}
    row.update(overrides)
    return row


def test_record_set_with_recorder_key():
    # Непустой набор: движения разбираются как обычно, Recorder_Key приходит из строк набора.
    reader = _reader(_FIELDS)
    reader._get_register_records(REG, {
        "d:Recorder_Key": REC,
        "d:RecordSet": {"@m:type": f"Collection(StandardODATA.{REG}_RowType)",
                        "d:element": [_row()]}})

    data = reader[REG].data
    assert data["Recorder_Key"] == [uuid.UUID(REC)]
    assert data["Period"] == [datetime(2026, 1, 1, 0, 0, 5)]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [False]
    assert data[EXCHANGE_MESSAGE_NO_FIELD] == [7]


@pytest.mark.parametrize("record_set", [
    {"@m:type": f"Collection(StandardODATA.{REG}_RowType)"},   # <d:RecordSet .../> без элементов
    {"@m:type": f"Collection(StandardODATA.{REG}_RowType)", "d:element": None},
    None,                                                       # <d:RecordSet/>
])
def test_empty_record_set_with_recorder_key_is_deleted(record_set):
    # Пустой набор = набор записей удалён: фиктивная строка с реальным регистратором,
    # полным (не NULL) ключом и is_deleted_or_empty=True — раньше отсюда прилетал Period=NULL.
    reader = _reader(_FIELDS)
    reader._get_register_records(REG, {"d:Recorder_Key": REC, "d:RecordSet": record_set})

    data = reader[REG].data
    assert data["Recorder_Key"] == [uuid.UUID(REC)]
    assert data["Period"] == [datetime(1, 1, 1)]
    assert data["Customer_Key"] == [EMPTY_UUID]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [True]
    assert data[EXCHANGE_MESSAGE_NO_FIELD] == [7]


def test_empty_record_set_with_composite_recorder_is_deleted():
    # Составной регистратор (Recorder + Recorder_Type) — прежнее поведение сохраняется.
    fields = {"Recorder": "Guid", "Recorder_Type": "String", "Period": "DateTime"}
    reader = _reader(fields, primary_key={"Recorder": "Guid", "Recorder_Type": "String",
                                          "Period": "DateTime"})
    reader._get_register_records(REG, {
        "d:Recorder": REC, "d:Recorder_Type": "StandardODATA.Document_Order",
        "d:RecordSet": {"@m:type": f"Collection(StandardODATA.{REG}_RowType)"}})

    data = reader[REG].data
    assert data["Recorder"] == [uuid.UUID(REC)]
    assert data["Recorder_Type"] == ["Document_Order"]
    assert data["Period"] == [datetime(1, 1, 1)]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [True]


def test_independent_register_still_flat():
    # Независимый регистр сведений: RecordSet отсутствует, поля лежат прямо в properties.
    fields = {"Period": "DateTime", "Customer_Key": "Guid", "PlannedQuantity": "Int64"}
    reader = _reader(fields, primary_key={"Period": "DateTime", "Customer_Key": "Guid"})
    reader._get_register_records(REG, {
        "d:Period": "2026-01-01T00:00:05",
        "d:Customer_Key": "5e51e8e9-6821-11ec-a232-00155de3390c",
        "d:PlannedQuantity": "50"})

    data = reader[REG].data
    assert data["Period"] == [datetime(2026, 1, 1, 0, 0, 5)]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [False]


def test_empty_reference_in_key_is_not_null():
    # Пустая ссылка 1С в поле ключа остаётся нулевым GUID (колонка ключа NOT NULL),
    # а вне ключа по-прежнему превращается в NULL.
    fields = dict(_FIELDS, Manager_Key="Guid")
    reader = _reader(fields)
    reader._get_register_records(REG, {
        "d:Recorder_Key": REC,
        "d:RecordSet": {"d:element": [_row(**{
            "d:Customer_Key": "00000000-0000-0000-0000-000000000000",
            "d:Manager_Key": "00000000-0000-0000-0000-000000000000"})]}})

    data = reader[REG].data
    assert data["Customer_Key"] == [EMPTY_UUID]   # в ключе
    assert data["Manager_Key"] == [None]          # вне ключа


def test_object_key_uses_recorder_key():
    # scoped-удаление группы должно идти по Recorder_Key, иначе набор регистратора не заменяется.
    metadata = MetadataReader1C(odata_url="http://fake")
    assert metadata._get_object_key(REG, _FIELDS, _PRIMARY_KEY) == ["Recorder_Key"]
    assert metadata._get_object_key(
        REG, {"Recorder": "Guid", "Recorder_Type": "String"}, {}) == ["Recorder", "Recorder_Type"]
