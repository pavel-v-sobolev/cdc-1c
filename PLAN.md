# План проекта cdc-1c

## Context

Ядро CDC из 1С (OData → БД через dbmerge) реализовано и проверено на живой 1С + PostgreSQL:
чтение изменений (`ChangeReader1C`), разбор в колоночные `DataObject1C`, транслитерация и лимиты
имён (`NameMapper1C`), типы/ключи/`delete_key` из метаданных (`MetadataReader1C`), пер-объектное
сохранение со scoped-delete для регистров/ТЧ (`DBWriter1C`), спец-поля `is_deleted_or_empty` и
`exchange_message_no`.

Текущая цель: оформить наработку как opensource-продукт — Python-пакет на PyPI + Docker-образ для
запуска «из коробки» с минимумом настроек, и удобный оркестратор с простым вызовом.

## Решения
- Имя: дистрибутив **cdc-1c**, import-пакет **cdc_1c** (нижний регистр); классы PascalCase.
- Оркестратор: класс **`Replicator1C`** (суффикс `1C` — как у остальных публичных классов).
- Конструктор: **отдельные именованные аргументы** (без объекта настроек) — прямой библиотечный
  вызов без обёртки, единый стиль с прочими классами. БД передаётся готовым **`engine`** (DI по-
  sqlalchemy'ному, тестируемость, переиспользование), а не строкой; тот же engine идёт в `DBWriter1C`.
- Объекта настроек нет: и entrypoint (`python -m cdc_1c`), и пользовательский runner.py читают
  окружение сами, присваивают аргументы явно и сами строят engine — так на месте вызова видно, что
  именно передано, одинаково для python-приложения и для контейнера.
- Режимы оркестратора: `run_once()` и `run_forever(interval)`.
- Python: **>=3.10**. Лицензия: **MIT** (предварительно).

---

## Сделано (ядро CDC)

- **`_get_register_records` / удаление наборов** — `_default_key_value`, `_make_deleted_register_record`:
  при пустом `RecordSet` запись дополняется полным ключом из метаданных (дефолты) + реальные
  `Recorder`/`Recorder_Type`.
- **`NameMapper1C`** — транслитерация; лимит `POSTGRES_MAX_IDENTIFIER = 63` (обрезка + 4-симв. хэш);
  служебные имена `RESERVED_FIELD_NAMES` (`merged_on`, `inserted_on`, `exchange_message_no`): наше
  служебное поле сохраняет имя, поле 1С с таким же транслитом хэшируется; журнал
  `object_mappings`/`field_mappings`. Ручной маппинг убран.
- **`DataReader1C`** — `DataObject1C.to_records_mapped()` (dict-of-lists → list-of-dict, маппинг
  колонок на лету, без копии); `_convert_value` для `Guid` → `uuid.UUID`.
- **`DBWriter1C`** — пер-объектное сохранение; удаление по `metadata_obj.delete_key`: документ/
  справочник → `delete_mode='no'`; регистр/ТЧ → `delete_mode='delete'` со scoped `delete_condition`
  (`_scoped_delete_condition`: подзапрос из temp-таблицы — `col IN (SELECT col FROM temp)` или
  row-value `tuple_(...).in_(select(...))`). `data_reader` в конструкторе, `save_all()`.
- **`MetadataReader1C`** — `MetadataObject1C.delete_key` + `_get_delete_key` (регистр →
  `Recorder`/`Recorder_Type`; ТЧ → `Ref_Key`; документ/справочник → None).
- **Табличные части** — строкам ТЧ проставляется `Ref_Key` владельца (в данных 1С его нет);
  пустая ТЧ → фиктивная запись (`_make_empty_table_part_record`) для scoped-delete по `Ref_Key`.
- **Спец-поля** — `is_deleted_or_empty` (Boolean: `DeletionMark` документа/справочника, проброс
  пометки в строки ТЧ, `True` у фиктивных записей) и `exchange_message_no` (номер пакета обмена,
  во всех записях; ставится в `ChangeReader.read_changes`).
- **A1. Переименование** — `src/cdc_1C` → `src/cdc_1c`, все импорты `cdc_1C` → `cdc_1c`,
  `pyproject` `name = "cdc-1c"`, `version("cdc-1c")`. Пересобрано `uv sync` (cdc-1c==0.1.0).

---

## Осталось

### Фаза A. Hardening ядра
- **A2. Конфигурируемый auth** — ✅ (частично) `MetadataReader1C`/`DataReader1C.__init__` принимают
  `auth: tuple[str,str] | None`, хранят `self.auth`; все `requests.*` используют `auth=self.auth`;
  `ChangeReader1C` пробрасывает `auth`. Захардкоженные `('admin','admin')` убраны (4 места).
  Осталось: общий `requests.Session`, `timeout`, опц. `verify`.
- **A3. Сброс состояния** — ✅ `ChangeReader1C.read_changes()` в начале делает `self.clear()`.
- **A4. Настройки из окружения** — ✅ разбор `CDC1C_*` живёт в `src/cdc_1c/__main__.py` (отдельный
  класс настроек убран как лишний слой). Осталось: опц. `request_timeout`, `verify_ssl` (с остатком A2).
- **A5. Оркестратор `Replicator1C`** — ✅ `src/cdc_1c/replicator.py`: собирает
  metadata/changes/mapper/writer из отдельных аргументов (engine + auth из user/password);
  `run_once()` (read → save → **notify только после
  успешного save**); `run_forever(interval)` с обработкой исключений (упал цикл → лог, без notify,
  повтор) и graceful SIGTERM/SIGINT (`_StopSignal`). Экспортирован из `cdc_1c`.
- **A6. Логирование** — ✅ во всех модулях `logger = logging.getLogger(__name__)`, `basicConfig()`/root
  убраны (ридеры/writer/replicator). Авто-вывод из коробки — `src/cdc_1c/logging_config.py`
  `_ensure_handler()` (вешает StreamHandler на логгер `cdc_1c` с INFO, только если `hasHandlers()` ==
  False), вызывается из `Replicator1C.__init__`. Если приложение настроило логирование — молчим.
  Уровень из `CDC1C_LOG_LEVEL` — настраивается в entrypoint (B2). Проверено: на чистом root INFO виден
  из коробки, при настроенном приложением логировании — молчим. Зависимость **dbmerge** переведена на
  тот же паттерн (`getLogger('dbmerge')` + `hasHandlers()`, без `basicConfig`) — root больше не
  загрязняется, обе библиотеки сосуществуют чисто.
- ✅ Файлы модулей в snake_case (`metadata_reader.py`, `data_reader.py`, `change_reader.py`,
  `name_mapper.py`, `db_writer.py`); классы и реэкспорт из `__init__` не изменены, публичный API
  (`from cdc_1c import …`) прежний.

### Фаза B. Упаковка для PyPI
- **B1. `pyproject.toml`** — описание, `license = "MIT"`, авторы, `requires-python = ">=3.10"`,
  keywords/classifiers/urls; ядро: `dbmerge`, `requests`, `sqlalchemy`, `xmltodict` (убрать `polars`
  и `psycopg2` из обязательных); `[project.optional-dependencies] postgres = ["psycopg[binary]>=3.2"]`
  (psycopg3, DSN `postgresql+psycopg`); `[project.scripts] cdc-1c = "cdc_1c.__main__:main"`.
- **B2. Entrypoint** — `src/cdc_1c/__main__.py`: `CDC1C_*` → явные аргументы `Replicator1C`; режим
  `CDC1C_MODE=once|loop` (по умолчанию loop). Работает как `python -m cdc_1c` и команда `cdc-1c`.
- **B3.** `LICENSE`, `src/cdc_1c/py.typed`, `CHANGELOG.md`; английский README (quickstart pip+Docker,
  таблица ENV, требования к плану обмена OData в 1С, поведение имён, спец-поля, ограничения);
  `tests/debug.py` — отладочный вход для ручных прогонов против живой 1С.

### Фаза C. Docker «из коробки»
- **C1. `Dockerfile`** — `python:3.12-slim`, `cdc-1c[postgres]`, `ENTRYPOINT ["cdc-1c"]`, ENV,
  graceful SIGTERM.
- **C2. `docker-compose.yml`** — сервис `cdc-1c` + опциональный `postgres`; `.env.example`;
  README-раздел «Запуск в Docker за 1 минуту».

### Фаза D. Качество
- **D1. Тесты (pytest)** — `NameMapper1C`, `to_records_mapped`, `_get_delete_key`; на sqlite —
  `DBWriter1C` scoped-delete (регистр/ТЧ/пустая ТЧ), идемпотентность, фиктивные записи,
  `is_deleted_or_empty` по всем веткам, GUID→`uuid.UUID`; ридеры — фикстуры реального XML
  `SelectChanges`/`$metadata`.
- **D2. CI (GitHub Actions)** — ruff, тесты на матрице 3.10–3.13, сборка; публикация на PyPI по
  git-тегу (OIDC/trusted publishing).

---

## Известные ограничения (→ README)
- Фиктивные записи (удалённый набор регистра, пустая ТЧ) вставляются как заглушка с дефолтным
  ключом (`LineNumber=0`); реальные старые строки удаляются scoped-delete'ом. Отличаются по
  `is_deleted_or_empty=True`.
- Опустевшая ТЧ как `xsi:nil` сейчас отфильтровывается (`_get_record_table_parts`) — уточнить формат
  1С и допокрыть при необходимости.
- Уникальность обрезанных длинных имён строго не гарантируется (хэш от полного имени).

## Проверка (end-to-end)
1. `uv sync`; `pytest` — зелёные offline-тесты (без 1С/Postgres).
2. `Replicator1C(...).run_once()` против живой 1С + Postgres: таблицы, типы,
   scoped-delete, спец-поля; повторный прогон — идемпотентность.
3. `run_forever(interval=…)`: цикл чистит состояние, notify только после успешного save, реакция на SIGTERM.
4. Docker: `docker compose up` с заполненным `.env` → данные грузятся без правок кода; `docker stop` штатно завершает.
5. `uv build` + установка из wheel в чистом окружении (3.10): импорт и `cdc-1c`/`python -m cdc_1c`.
