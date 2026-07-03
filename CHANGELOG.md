# Changelog

Формат — [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/),
версионирование — [SemVer](https://semver.org/lang/ru/).

## [0.1.0] — Unreleased

Первый публичный релиз (Development Status :: Alpha).

### Added
- Оркестратор `Replicator1C`: чтение изменений из 1С (OData + план обмена) и upsert в целевую БД
  через `dbmerge`; подтверждение приёма пакета только после успешного сохранения (`run_once` /
  `run_forever` с graceful SIGTERM/SIGINT).
- Полная выгрузка `full_load`: keyset-пагинация (Ref_Key / Recorder / составной ключ независимого
  регистра), фильтр по периоду (`date_field` + `date_from`/`date_to`), фоновые выгрузки в
  `run_forever`.
- Version-guard полной выгрузки (`exchange_message_no`): устаревший снимок не затирает более свежие
  изменения и не воскрешает удалённые строки групп (регистр/табличная часть).
- Транслитерация имён и лимиты идентификаторов (`NameMapper1C`); спец-поля `merged_on`,
  `inserted_on`, `is_deleted_or_empty`, `exchange_message_no`.
- Служебные таблицы: журнал загрузок `replicator_1c_log`, реестр объектов `metadata_objects_1c`.

[0.1.0]: https://github.com/pavel-v-sobolev/cdc_1C/releases/tag/v0.1.0
