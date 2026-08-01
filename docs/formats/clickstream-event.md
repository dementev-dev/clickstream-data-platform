# Описание выгрузки: событие кликстрима

Документ собран из контракта схемы генератора
(`generator/src/clickstream_generator/schema.py`). Руками не править —
пересобрать: `make docs`.

Одно событие — одна строка: хит по образцу облачной выгрузки Яндекс Метрики.
Многозначное лежит в параллельных массивах одной длины, плюс одно сырое
JSON-поле `ecommerce`. Отдельной сущности «визит» в выгрузке нет — визиты
собирают на стороне хранилища, а `VisitID` дан как эталон для самопроверки.

Имена и типы колонок — стороны источника. Хранилище принимает их как есть и
нормализует у себя: своё snake_case-имя каждой колонки ждёт в столбце «Имя в
DDS». Столбец «Тип numpy» показывает, чем колонка представлена внутри
генератора; у массивов это тип элемента. Номер — место колонки в выгрузке:
порядок задан контрактом.

Колонки группы «Ecommerce» заполнены только у торговых событий:
`add_to_cart` несёт один товар, `purchase` — состав заказа и блок
`purchase*`. У остальных событий они пусты.

Всего колонок: 47.

## Идентификаторы и время

| № | Колонка | Тип ClickHouse | Тип numpy | Имя в DDS | Комментарий |
|---|---|---|---|---|---|
| 1 | `WatchID` | `UInt64` | `uint64` | `watch_id` | id события — хита; держится ниже 2^53, выше числа в JSON округляются |
| 2 | `VisitID` | `UInt64` | `uint64` | `visit_id` | id визита от генератора — эталон лабы: собери сессии сам и сравни |
| 3 | `ClientID` | `UInt64` | `uint64` | `client_id` | анонимный id браузера — кука; по хешу от неё таблица шардируется |
| 4 | `CounterID` | `UInt32` | `uint32` | `counter_id` | id счётчика: на стенде константа, сайт один |
| 5 | `EventDate` | `Date` | `datetime64[D]` | `event_date` | дата события; по ней режется партиция |
| 6 | `UTCEventTime` | `DateTime` | `datetime64[s]` | `utc_event_time` | время события в UTC — единственная метка времени, как у Метрики |
| 7 | `ClientTimeZone` | `Int16` | `int16` | `client_timezone` | смещение часового пояса клиента от UTC, в минутах |
| 8 | `EventType` | `LowCardinality(String)` | `object` | `event_type` | тип события: pageview, add_to_cart, purchase — добавка стенда, у Метрики такого поля нет |
| 9 | `Sign` | `Int8` | `int8` | `sign` | всегда 1: колонка формата, исправлений записей генератор не шлёт |

## Страница и атрибуция

| № | Колонка | Тип ClickHouse | Тип numpy | Имя в DDS | Комментарий |
|---|---|---|---|---|---|
| 10 | `URL` | `String` | `object` | `url` | адрес страницы события |
| 11 | `Referer` | `String` | `object` | `referer` | адрес, с которого посетитель пришёл на страницу |
| 12 | `Title` | `String` | `object` | `title` | заголовок страницы |
| 13 | `UTMSource` | `String` | `object` | `utm_source` | метка utm_source: площадка перехода |
| 14 | `UTMMedium` | `String` | `object` | `utm_medium` | метка utm_medium: тип трафика |
| 15 | `UTMCampaign` | `String` | `object` | `utm_campaign` | метка utm_campaign: рекламная кампания |
| 16 | `UTMContent` | `String` | `object` | `utm_content` | метка utm_content: что различает объявления одной кампании |
| 17 | `UTMTerm` | `String` | `object` | `utm_term` | метка utm_term: ключевое слово перехода |
| 18 | `LastTrafficSource` | `String` | `object` | `last_traffic_source` | последний источник трафика: organic, direct, ad и подобные |
| 19 | `HasGCLID` | `UInt8` | `uint8` | `has_gclid` | 1, если в адресе была метка Google Ads |
| 20 | `YCLID` | `UInt64` | `uint64` | `yclid` | id клика Яндекс Директа; без метки — 0 |

## Браузер, устройство, гео

| № | Колонка | Тип ClickHouse | Тип numpy | Имя в DDS | Комментарий |
|---|---|---|---|---|---|
| 21 | `Browser` | `String` | `object` | `browser` | браузер посетителя |
| 22 | `BrowserMajorVersion` | `UInt16` | `uint16` | `browser_major_version` | старшая версия браузера |
| 23 | `BrowserLanguage` | `String` | `object` | `browser_language` | язык браузера |
| 24 | `OperatingSystem` | `String` | `object` | `operating_system` | операционная система с версией |
| 25 | `OperatingSystemRoot` | `String` | `object` | `operating_system_root` | семейство операционной системы, без версии |
| 26 | `DeviceCategory` | `UInt8` | `uint8` | `device_category` | тип устройства кодами Метрики: 1 — десктоп, 2 — телефон, 3 — планшет, 4 — телевизор; у Метрики это строка, у нас число |
| 27 | `MobilePhoneModel` | `String` | `object` | `mobile_phone_model` | модель телефона; на десктопе пусто |
| 28 | `ScreenWidth` | `UInt16` | `uint16` | `screen_width` | ширина экрана в пикселях |
| 29 | `ScreenHeight` | `UInt16` | `uint16` | `screen_height` | высота экрана в пикселях |
| 30 | `IPAddress` | `String` | `object` | `ip_address` | IP-адрес посетителя |
| 31 | `RegionCountry` | `String` | `object` | `region_country` | страна кодом ISO |
| 32 | `RegionCity` | `String` | `object` | `region_city` | город, название по-английски |
| 33 | `RegionCountryID` | `UInt32` | `uint32` | `region_country_id` | числовой id страны в справочнике регионов Яндекса |
| 34 | `RegionCityID` | `UInt32` | `uint32` | `region_city_id` | числовой id города в том же справочнике |

## Массивы и параметры

| № | Колонка | Тип ClickHouse | Тип numpy | Имя в DDS | Комментарий |
|---|---|---|---|---|---|
| 35 | `GoalsReached` | `Array(UInt32)` | `uint32` | `goals_reached` | id достигнутых целей; на стенде их две — корзина и покупка |
| 36 | `ParsedParamsKey1` | `Array(String)` | `object` | `parsed_params_key1` | свои параметры сайта, один уровень — например вариант A/B-теста |

## Ecommerce

| № | Колонка | Тип ClickHouse | Тип numpy | Имя в DDS | Комментарий |
|---|---|---|---|---|---|
| 37 | `purchaseID` | `Array(String)` | `object` | `purchase_id` | номер заказа; у события purchase — один элемент |
| 38 | `purchaseRevenue` | `Array(Float64)` | `float64` | `purchase_revenue` | выручка заказа глазами клиента; Float64, как у Метрики — на этом держится урок о расхождениях с бэкендом |
| 39 | `purchaseCurrency` | `Array(String)` | `object` | `purchase_currency` | валюта заказа |
| 40 | `purchaseCoupon` | `Array(String)` | `object` | `purchase_coupon` | купон заказа, если был применён |
| 41 | `productID` | `Array(String)` | `object` | `product_id` | id товаров события |
| 42 | `productName` | `Array(String)` | `object` | `product_name` | названия тех же товаров |
| 43 | `productCategory` | `Array(String)` | `object` | `product_category` | категории тех же товаров |
| 44 | `productPrice` | `Array(Int64)` | `int64` | `product_price` | цена за штуку целым числом: деньги генератор считает целыми |
| 45 | `productQuantity` | `Array(UInt64)` | `uint64` | `product_quantity` | количество штук каждого товара |
| 46 | `productEventType` | `Array(String)` | `object` | `product_event_type` | действие с товаром: detail, add, remove, purchase |
| 47 | `ecommerce` | `String` | `object` | `ecommerce` | сырой JSON события, как отдаёт Метрика — материал лабы про разбор JSON внутри колонки |
