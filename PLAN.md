# План: пер-объектное сохранение изменений 1С в БД через dbmerge

## Context

Проект `cdc_1C` выгружает изменения объектов из 1С по OData (`ChangeReader1C.read_changes()`)
и сохраняет их в БД. Чтение и трансформация были готовы (`DataReader1C` → колоночные
`DataObject1C`, `NameMapper1C` — транслитерация, `MetadataReader1C` — типы и ключи), не хватало
сохранения в БД (`dbmerge`). 1С `SelectChanges` не поддерживает server-side пагинацию, поэтому
единицей обработки выбран **один 1С-объект**: `read_changes()` за один запрос тянет все изменения
и раскладывает их по `DataObject1C` (по одному на тип объекта), затем мы итерируемся по объектам и
для каждого делаем маппинг имён + сохранение через `dbmerge`.

Целевая БД — PostgreSQL. Многопоточность сейчас не делаем (см. «На будущее»).

## Статус

Сделано: п.0–4, спец-поле `is_deleted_or_empty`, delete_key/scoped-delete.
Осталось: п.5 (оркестрация в main.py) и зависимость `psycopg`.

## Сделано

### 0. `_get_register_records` — полный ключ удалённых записей  ✅
Файл: `src/cdc_1C/DataReader1C.py`
- `_default_key_value(type_name)` — дефолты: `''` (String), `0` (Int64/Int16/Double),
  `False` (Boolean), `datetime(1,1,1)` (DateTime), `None` (прочее, в т.ч. Guid).
- `_make_deleted_register_record(object_name, recorder, recorder_type)` — при пустом `RecordSet`
  собирает запись с полным первичным ключом из метаданных (дефолты) и реальными
  `Recorder`/`Recorder_Type`.

### 1. `NameMapper1C` — без ручного маппинга, лимиты PostgreSQL  ✅
Файл: `src/cdc_1C/NameMapper1C.py`
- Убран `manual_mapping`.
- Лимит идентификатора `POSTGRES_MAX_IDENTIFIER = 63`: длинное имя обрезается и дополняется
  `_<4-симв. md5>` (итог ≤ 63). Применяется к `map_object_name` и `map_field_name`.
- `RESERVED_FIELD_NAMES = ('merged_on', 'inserted_on', 'change_message_no')`: если поле 1С после
  транслитерации совпало со служебным именем — добавляется хэш.
- Журнал `object_mappings` / `field_mappings` (оригинал → результат) для отладки.

### 2. `DataReader1C` — материализация строк и GUID  ✅
Файл: `src/cdc_1C/DataReader1C.py`
- `DataObject1C.to_records_mapped(column_mapping=None)` — dict of lists → list of dict; маппинг
  колонок применяется на лету (без копии данных, значения переиспользуются по ссылке).
- `_convert_value` для `Guid` возвращает `uuid.UUID(value)` (zero-GUID → `None`) — корректный bind
  в колонки `Uuid` PostgreSQL.

### 3. `DBWriter1C` — сохранение объекта через dbmerge  ✅
Файл: `src/cdc_1C/DBWriter1C.py`
- `save(object_name, data_object)`: пропускает пустые/без метаданных; маппит имя таблицы и колонок,
  собирает `records` через `to_records_mapped`, берёт `key`/`data_types` из метаданных (мапленные имена).
- **Удаление по типу объекта** через `metadata_obj.delete_key`:
  - `delete_key` пуст (документ/справочник, один Ref_Key) → `delete_mode='no'`;
  - `delete_key` задан (регистр/ТЧ) → `delete_mode='delete'` со scoped `delete_condition`
    (`_scoped_delete_condition`): один столбец → `col.in_(...)`, составной → `tuple_(...).in_(...)`
    (row-value `(Recorder, Recorder_Type) IN ((..),..)`). Удаляются только строки групп, пришедших
    в наборе и отсутствующих в источнике.
- `save_all(data_reader)`: последовательно по объектам; исключения пробрасываются (чтобы не
  подтверждать изменения при неполном сохранении).

### 3a. `delete_key` в метаданных  ✅
Файл: `src/cdc_1C/MetadataReader1C.py`
- `MetadataObject1C` получил поле `delete_key`.
- `_get_delete_key(item_name, properties, primary_key)`: регистр → `['Recorder','Recorder_Type']`;
  табличная часть (ключ `Ref_Key` + ещё поля) → `['Ref_Key']`; документ/справочник → `None`.
- Проставляется в обеих ветках `get_metadata`.

### 3b. Фиктивная запись для пустой табличной части  ✅
Файл: `src/cdc_1C/DataReader1C.py`
- `_make_empty_table_part_record(table_part_name, ref_key)` — по аналогии с удалённым набором
  регистра: полный ключ дефолтами + реальный `Ref_Key` владельца.
- `_get_entity_records`: если ТЧ пришла без строк → добавляется фиктивная запись (для scoped-delete
  по `Ref_Key`).
- Не покрыто: если 1С присылает опустевшую ТЧ как `xsi:nil`, `_get_record_table_parts` её
  отфильтровывает. Уточнить формат и допокрыть при необходимости.

### 3c. Спец-поле `is_deleted_or_empty` (Boolean)  ✅
Файл: `src/cdc_1C/DataReader1C.py`
- Заполняется при загрузке: документы/справочники → `bool(DeletionMark)`; регистры/ТЧ (нет
  `DeletionMark`) → `False`; фиктивные записи (удалённый набор регистра, пустая ТЧ) → `True`.
- Не входит в метаданные (тип Boolean определит dbmerge), в PK/`delete_key` не входит.
- В `RESERVED_FIELD_NAMES` намеренно не добавлено: это реальная колонка данных, должна сохранять
  литеральное имя. Если нужна страховка от коллизии с одноимённым полем 1С — отдельная доработка.

### 4. Экспорт  ✅
- `src/cdc_1C/__init__.py`: `DBWriter1C` добавлен в импорты и `__all__`.

## Осталось

### 5. Оркестрация и зависимость
Файлы: `main.py`, `pyproject.toml`
- В `pyproject.toml` добавить драйвер `psycopg` (psycopg3).
- В `main.py`: создать engine
  `create_engine("postgresql+psycopg://<user>:<pass>@<host>/<db>")` (DSN из env), после
  `changes.read_changes()`:
  ```python
  writer = DBWriter1C(engine, mapper)
  writer.save_all(changes)
  changes.notify_changes_received()   # только после успешного сохранения всех объектов
  ```
- Для отладки вывести `mapper.object_mappings` / `mapper.field_mappings`.

## Известные ограничения
- **Фиктивные записи** (удалённый набор регистра, опустевшая ТЧ) вставляются как «заглушка» с
  дефолтным ключом (`LineNumber=0`); реальные старые строки при этом корректно удаляются scoped-
  delete'ом. Их можно отличать по `is_deleted_or_empty=True`. Полностью чистое удаление без вставки
  заглушки — отдельная доработка при необходимости.
- **Уникальность обрезанных имён**: при обрезке длинного имени добавляется хэш от полного имени —
  коллизия крайне маловероятна, но строгая гарантия не реализована.

## Проверка (end-to-end)
1. PostgreSQL + DSN; `uv sync` (подтянуть `psycopg`).
2. `python main.py` против рабочего `base_url` 1С и тестовой БД.
3. В БД: таблицы с латинскими именами, корректные типы (Uuid/Numeric/DateTime/Boolean), строки
   вставлены/обновлены по ключу; для регистров/ТЧ старые строки заменённых групп удалены;
   `is_deleted_or_empty` проставлен.
4. Повторный прогон тех же изменений — идемпотентность (0 inserted).
5. `mapper.object_mappings` / `mapper.field_mappings` — старые и новые имена видны.

Offline-проверки на `sqlite:///` уже прогонялись и прошли: `to_records_mapped`, scoped-delete по
регистру (составной ключ) и по ТЧ (Ref_Key), пустая ТЧ, заполнение `is_deleted_or_empty` по всем
веткам. Нюанс: на sqlite UUID не переживает повторный прогон (тип отражается как CHAR(32)) — это
ограничение sqlite, на PostgreSQL `Uuid` работает корректно.
