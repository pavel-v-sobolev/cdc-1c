**cdc-1c** is a Python library and a ready to use docker container, that provides data loading to data warehouse using Change Data Capture apporach. \
It engages standard ODATA mechanism and standard 1C exchange plan mechanism to extract data from 1C system and upsert changes to the target DB.

**cdc-1c** - это python-библиотека и готовый к использованию docker-контейнет, предназначенные для получения данных из 1С, использующий подход CDC (загрузка изменений данных). \
Продукт использует стандартный интерфейс ODATA и механизм планов обмена для выгрузки изменений данных из системы 1С и обновления данных в целевой БД.

# Что нужно для работы
1) опубликовать базу 1с на web
2) создать пользователя odata и дать ему необходимые права
3) настроить узел обмена с использованием внешней обработки `cdc-1c.odt`
4) поднять docker контейнер для получения данных или адаптировать под свой вариант, исспользуя библиотеку python cdc-1c
5) инициировать начальную загрузку данных в `cdc-1c.odt`

# Использование библиотеки python
