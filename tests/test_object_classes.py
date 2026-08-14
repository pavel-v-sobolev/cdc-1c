"""
Оффлайн-тесты разбора классов объектов 1С (read_data_entries).

Ссылочные классы (справочники, ПВХ, планы счетов, ПВР, бизнес-процессы, задачи) устроены одинаково
и разбираются общим кодом. Объекты классов, которые мы не умеем сохранять, пропускаются с ошибкой
в логе: пакет изменений подтверждается целиком, поэтому такие изменения теряются безвозвратно.
"""

import logging

import pytest

from cdc_1c import DataReader1C, MetadataReader1C
from cdc_1c.data_reader import IS_DELETED_OR_EMPTY_FIELD
from cdc_1c.db_writer import save_order_key
from cdc_1c.metadata_reader import MetadataObject1C

REF = "5e51e8e9-6821-11ec-a232-00155de3390c"


def _entry(object_full_name: str, properties: dict) -> dict:
    return {"category": {"@term": f"StandardODATA.{object_full_name}"},
            "content": {"m:properties": properties}}


def _reader(*objects: str) -> DataReader1C:
    metadata = MetadataReader1C(odata_url="http://fake")
    for name in objects:
        metadata[name] = MetadataObject1C(
            name, {"Ref_Key": "Guid", "Description": "String", "DeletionMark": "Boolean"},
            {"Ref_Key": "Guid"})
    metadata.is_loaded = True
    reader = DataReader1C(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 7
    return reader


@pytest.mark.parametrize("object_name", [
    "ChartOfCharacteristicTypes_Vidy",
    "ChartOfAccounts_Hozraschetnyi",
    "ChartOfCalculationTypes_Osnovnye",
    "BusinessProcess_Soglasovanie",
    "Task_Poruchenie",
])
def test_reference_classes_are_parsed_like_catalogs(object_name):
    reader = _reader(object_name)

    reader.read_data_entries([_entry(object_name, {"d:Ref_Key": REF, "d:Description": "X",
                                                   "d:DeletionMark": "false"})])

    data = reader[object_name].data
    assert data["Description"] == ["X"]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [False]


def test_unsupported_class_is_skipped_and_reported(caplog):
    # Регистр бухгалтерии пока не поддерживается: устроен иначе (Dr/Cr-пары, счета).
    reader = _reader()

    with caplog.at_level(logging.ERROR):
        parsed = reader.read_data_entries([
            _entry("AccountingRegister_Hozraschetnyi", {"d:Recorder": REF}),
            _entry("AccountingRegister_Hozraschetnyi", {"d:Recorder": REF}),
        ])

    assert "AccountingRegister_Hozraschetnyi" not in reader   # не сохранён
    assert parsed["AccountingRegister_Hozraschetnyi"] == 2     # но сосчитан
    text = caplog.text
    assert "unsupported" in text and "lost" in text
    assert "2 entries" in text                                 # один лог на пакет, с количеством


def test_reference_classes_are_saved_before_documents():
    # Документы ссылаются на ссылочные объекты, регистры — на документы.
    assert save_order_key("ChartOfAccounts_X") < save_order_key("Document_X")
    assert save_order_key("Task_X") < save_order_key("Document_X")
    assert save_order_key("Document_X") < save_order_key("AccumulationRegister_X")
