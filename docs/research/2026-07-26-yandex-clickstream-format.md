# Формат кликстрима Яндекса: что копировать стенду

Status: Research
Дата: 2026-07-26
Тикет: [#27](https://github.com/dementev-dev/clickstream-ch-kafka-superset-demo/issues/27)

## Зачем это исследование

Владелец стенда выбирает целевую модель данных. Сейчас событие разрезано на
четыре топика (`browser`, `location`, `device`, `geo`), которые склеиваются по
`event_id` и `click_id`. Вопрос: переходить ли на одно широкое событие и по
какому образцу его строить.

Образец выбран — Яндекс. В России Метрика и AppMetrica — основной источник
кликстрима, и менти должен узнать формат, когда столкнётся с ним на работе.

Всё ниже — по документации Яндекса, ClickHouse, Snowplow, Segment и Amplitude.
Где источник найти не удалось, это написано прямо.

## Коротко: ответы на главные вопросы

**Форма — плоское ядро с массивами, не вложенный JSON.** У Яндекса нет
вложенных объектов вроде `context` или `properties`. Есть примерно 140 плоских
колонок на хит, и всё, что бывает «много раз в одном событии» (цели, товары,
покупки, свои параметры), лежит в **параллельных массивах**: `productID`,
`productName`, `productPrice` — три отдельных массива одной длины, а не массив
объектов. Именно эту форму стенду и стоит копировать.

**Доставка — батч для всех и поток для платных.** Обычный путь (Logs API) —
батч: запрос готовится, потом скачивается TSV-файл. Данные за текущий день
недоступны. В тарифе «Метрика Про» есть поток в управляемый ClickHouse с
задержкой до 15 минут. Kafka в этой картине нет ни в одном официальном
варианте.

**Kafka на стенде — учебная замена, и так это и надо называть.** Подробный
разбор — в разделе «Батч или поток».

Главные находки:

1. Метрика отдаёт **две разные сущности**: хиты (`hits`, просмотры страниц) и
   визиты (`visits`, сессии). Визит — уже свёрнутая сессия с массивами внутри.
2. **56 полей хитов — массивы** (по подсчёту на странице полей Logs API).
   Массивы — основной способ Яндекса выразить «много значений в одном событии».
3. **User agent строкой Метрика не отдаёт.** Отдаются уже разобранные поля:
   `browser`, `browserMajorVersion`, `operatingSystem`, `deviceCategory`.
   Поле `browser_user_agent` на стенде — наша выдумка, у Яндекса аналога нет.
4. **Координат в выгрузке Метрики нет.** Гео — это `regionCountry`,
   `regionCity` и числовые `regionCountryID`, `regionCityID`. Наши
   `geo_latitude` и `geo_longitude` тоже без аналога.
5. **Метки времени: серверная одна.** В хитовой выгрузке в облако есть
   `UTCEventTime` и смещение часового пояса клиента `ClientTimeZone` (в
   минутах). Отдельной клиентской метки времени нет. Разделение «время у
   клиента» и «время приёма на сервере» есть у AppMetrica
   (`event_datetime` против `event_receive_datetime`), не у Метрики.
6. **Поток даёт версии одной записи.** В потоковой выгрузке есть колонки `Sign`
   (Int8) и `HitVersion` / `VisitVersion`. Старая версия приходит со `Sign =
   -1`, новая с `Sign = 1`; складывать надо через `sum(Sign)`. Это ровно та же
   механика, что у движка `CollapsingMergeTree` в ClickHouse.
7. **Ecommerce и выручка в выгрузке есть, но плоско.** Сумма заказа —
   `purchaseRevenue` типа `Array(Float64)`, валюта — `purchaseCurrency` типа
   `Array(String)`. Плюс отдельно лежит поле `ecommerce` типа `String` — сырой
   JSON события ecommerce.

## 1. Logs API Метрики: состав данных

Источники: [список полей — хиты](https://yandex.ru/dev/metrika/ru/logs/fields/hits),
[список полей — визиты](https://yandex.ru/dev/metrika/ru/logs/fields/visits),
[введение в Logs API](https://yandex.ru/dev/metrika/ru/logs/).

### Две сущности вместо одной

- **Хит (`hits`)** — одно действие: просмотр страницы, клик по ссылке, скачивание
  файла. Имена полей начинаются с `ym:pv:`.
- **Визит (`visits`)** — сессия целиком. Имена полей начинаются с `ym:s:`.
  Внутри визита лежит массив идентификаторов его хитов: `ym:s:watchIDs` типа
  `Array(UInt64)`, не более 500 просмотров.

Это важно для стенда: у Яндекса «широкое событие» — это хит, а сессия —
отдельная широкая запись, собранная Яндексом на своей стороне. У нас сессии
считаются в DDS/DM; у Яндекса они приходят готовыми.

### Идентификаторы

| Поле | Тип | Что это |
|---|---|---|
| `ym:pv:watchID` | UInt64 | Идентификатор хита |
| `ym:pv:pageViewID` | UInt32 | Идентификатор просмотра страницы |
| `ym:pv:visitID` | UInt64 | Идентификатор визита (в хитах доступен с 10.10.2025) |
| `ym:pv:counterID` | UInt32 | Номер счётчика (то есть сайта) |
| `ym:pv:clientID` | UInt64 | Анонимный идентификатор посетителя через свою cookie |
| `ym:pv:counterUserIDHash` | UInt64 | Идентификатор посетителя для подсчёта уникальных |
| `ym:s:visitID`, `ym:s:clientID`, `ym:s:counterUserIDHash` | те же типы | То же на уровне визита |

Все идентификаторы — **числа**, не UUID.

Про `UserID` — свой идентификатор пользователя со стороны сайта. По справке
Метрики он передаётся методом `setUserID` и связывается с `ClientID`, если
метод вызвали во время сессии
([setUserID](https://yandex.com/support/metrica/en/objects/set-user-id.html)).
Поля `UserID` **в списке выгружаемых полей Logs API найти не удалось** — ни в
хитах, ни в визитах. В выгрузке хитов в облако его тоже нет. Считаем: свой
идентификатор пользователя в кликстриме Метрики напрямую не выдаётся.

Про рекламные метки:

- `ym:pv:hasGCLID` (UInt8) и `ym:pv:GCLID` (String) — метка клика Google.
- `ym:pv:hasSBCLID` (UInt8) и `ym:pv:SBCLID` (String) — метка SBCLID.
- **`YCLID` в списке полей Logs API не найден.** Он есть в примерном датасете
  ClickHouse как колонка `YCLID UInt64` (см. раздел 5) и упоминается в справке
  Метрики как идентификатор клика по объявлению Яндекс Директа. Вывод: в
  выгрузке Logs API поле `yclid` не подтверждено; в схеме ClickHouse оно есть.

### Метки времени и часовой пояс

| Поле | Тип | Что это |
|---|---|---|
| `ym:pv:date` | Date | Дата события |
| `ym:pv:dateTime` | DateTime | Дата и время события **в часовом поясе счётчика** |
| `ym:pv:clientTimeZone` | Int16 | Смещение часового пояса посетителя от UTC **в минутах** |
| `ym:s:dateTime` | DateTime | Время визита в часовом поясе счётчика |
| `ym:s:dateTimeUTC` | DateTime | Время визита в UTC+3 |

Две вещи, которые стоит запомнить:

- Часовой пояс — не UTC по умолчанию. У хита это «пояс счётчика», у визита есть
  и вторая колонка в UTC+3 (то есть по московскому времени, а не по нулевому
  меридиану).
- Клиентское время события отдельным полем не приходит. Приходит только
  **смещение пояса клиента**. То есть «две метки времени, клиентская и
  серверная» — это про AppMetrica и про западные трекеры, но не про Метрику.

### Массивы

Массивов очень много. Страница полей хитов помечает **56 полей как массивы**.
Основные группы:

- **Цели:** `ym:pv:goalsID` — `Array(UInt32)`. У визита целая группа
  параллельных массивов: `ym:s:goalsID`, `ym:s:goalsSerialNumber`,
  `ym:s:goalsDateTime`, `ym:s:goalsPrice`, `ym:s:goalsOrder`,
  `ym:s:goalsCurrency`.
- **Свои параметры:** `ym:pv:parsedParamsKey1` … `parsedParamsKey10`, каждый —
  `Array(String)`. Это десять уровней вложенности, разложенные по десяти
  массивам. Плюс поле `ym:pv:params` типа `String` — исходный JSON параметров.
- **Покупки:** `ym:pv:purchaseID` `Array(String)`, `ym:pv:purchaseRevenue`
  `Array(Float64)`, `ym:pv:purchaseCurrency` `Array(String)` и другие (в хитах
  доступны с 19.06.2025).
- **Товары:** `ym:pv:productID`, `productName`, `productBrand`,
  `productCategory`, `productCategoryLevel1`…`Level5`, `productPrice`
  `Array(Int64)`, `productQuantity` `Array(UInt64)`, `productEventType`
  `Array(String)` со значениями `view_item_list`, `click`, `detail`, `add`,
  `purchase`, `remove`.
- **Промоакции:** `ym:pv:promotionID`, `promotionName`, `promotionCreative`,
  `promotionEventType`.

У визита к этому добавляются ещё три больших блока массивов:
`ym:s:purchasedProduct*` (что купили), `ym:s:impressions*` (что показали) и
`ym:s:offlineCall*` (звонки).

Ключевое наблюдение: **это параллельные массивы, а не массив объектов**. Третий
товар в событии — это третий элемент в каждом из массивов `productID`,
`productName`, `productPrice`. Собирается такое в ClickHouse через `ARRAY JOIN`.

### Что ещё есть в хите (короткий список категорий)

- Страница: `ym:pv:URL`, `ym:pv:referer`, `ym:pv:title`, `ym:pv:pageCharset`.
- Атрибуция: `UTMSource`, `UTMMedium`, `UTMCampaign`, `UTMContent`, `UTMTerm`;
  `lastTrafficSource`, `lastSearchEngineRoot`, `lastSearchEngine`,
  `lastAdvEngine`, `lastSocialNetwork`; четыре поля Openstat; `ym:pv:from`.
- Браузер: `browser`, `browserMajorVersion`, `browserMinorVersion`,
  `browserEngine` и четыре части его версии, `browserLanguage`,
  `browserCountry`, `cookieEnabled`, `javascriptEnabled`.
- Устройство и экран: `deviceCategory` (1 = десктоп, 2 = телефон, 3 = планшет,
  4 = TV), `mobilePhone`, `mobilePhoneModel`, `operatingSystem`,
  `operatingSystemRoot`, `screenWidth`, `screenHeight`, `physicalScreenWidth`,
  `physicalScreenHeight`, `windowClientWidth`, `windowClientHeight`,
  `screenColors`, `screenFormat`, `screenOrientation`.
- Гео и сеть: `ipAddress`, `regionCountry` (код ISO), `regionCity` (название
  по-английски), `regionCountryID`, `regionCityID`.
- Признаки: `isPageView`, `isTurboPage`, `iFrame`, `link`, `download`,
  `notBounce`, `artificial`, `httpError`.

### Ограничения выгрузки

Со страницы [введения в Logs API](https://yandex.ru/dev/metrika/ru/logs/):

- Данные за **текущий день недоступны** — они могут быть неполными.
- Данные «доформировываются» примерно **до 3 дней**; запрашивать рекомендуют
  начиная с предыдущих дней.
- Максимальный период одного запроса — **1 год**.
- Параметр со списком полей — **не более 3000 символов**.
- Общая квота на объём подготовленных логов — **10 ГБ**. Подготовленные файлы
  надо регулярно удалять, иначе квота кончится. Квоту расширяет тариф
  «Метрика Про».
- Результат Logs API может расходиться с интерфейсом Метрики из-за разных
  алгоритмов обработки и округления чисел с плавающей точкой.

Ограничение «не более 1 ГБ на файл» встречается в поиске по документации
Метрики, но дословно подтвердить его на официальной странице ограничений не
удалось — **не подтверждено**.

Порядок работы (по официальным описаниям API):

1. `POST /management/v1/counter/{counterId}/logrequests` — создать запрос.
2. `GET  /management/v1/counter/{counterId}/logrequest/{requestId}` — проверить
   статус; статус `processed` означает, что лог готов.
3. `GET  .../logrequest/{requestId}/part/{partNumber}/download` — скачать
   часть.

Формат выгрузки — **TSV**. Дословно из официального блога Метрики: «Сырые
данные передаются в стандартном формате tsv»
([блог Метрики про Logs API](https://yandex.ru/blog/metrika/vygruzhayte-syrye-dannye-iz-metriki-cherez-logs-api)).
Там же прямо сказано, что типовой приёмник таких данных — ClickHouse.

## 2. Батч или поток

Коротко: **у Метрики есть оба пути, но Kafka нет ни в одном.**

### Путь 1. Logs API — батч

Запрос готовится, потом скачивается TSV-файл частями. Текущий день недоступен.
Это не поток ни в каком смысле: минимальная задержка — сутки.

### Путь 2. Data Streaming в Yandex Cloud — поток, но не Kafka

Источник: [Data Streaming (интеграция с Yandex
Cloud)](https://yandex.ru/support/metrica/ru/uploading-data/cloud), [как
работать с данными](https://yandex.ru/support/metrica/ru/pro/data-work),
[учебник Yandex
Cloud](https://yandex.cloud/ru/docs/tutorials/dataplatform/metrika-to-clickhouse).

Что там есть:

- Неагрегированные данные Метрики попадают в **свой управляемый
  ClickHouse-кластер** в Yandex Cloud.
- Задержка от события до записи в ClickHouse — **до 15 минут**.
- Перенос делает **Yandex Data Transfer**, тип трансфера — «Репликация».
- Хиты и визиты переносятся отдельными таблицами.
- Историю до создания коннектора эта версия не переносит. Если трансфер
  выключить и включить, данные за простой потеряются.
- Нужен тариф **«Метрика Про»**.
- Визит меняется по мере поступления новых событий, поэтому в выгрузке лежат
  **разные версии одного визита**. Разбираются они через `Sign`: старая версия
  приходит со `Sign = -1`, новая со `Sign = 1`, считать надо через `sum(Sign)`
  в `GROUP BY`. Можно использовать модификатор `FINAL`, но он медленнее.

Приёмник здесь — ClickHouse напрямую. **Экспорт кликстрима Метрики в Object
Storage или в Yandex Data Streams (сервис с Kafka-совместимым интерфейсом)
официальной документацией не подтверждён.**

### Путь 3. AppMetrica Data Stream — пятиминутные окна

Источник: [Data Stream API,
описание](https://appmetrica.yandex.ru/docs/ru/mobile-api/datastream/about).
Поток представлен последовательностью **пятиминутных окон**, каждое окно
скачивается запросом. Задержка — не меньше 10 минут. Хранение — 7 дней.
Формат — CSV (RFC 4180) или JSON.

### Честно ли рассказывать про Kafka

Да, если называть вещи своими именами. Формулировка для курса:

> Kafka на стенде — учебная замена реальному транспорту. Основной путь выгрузки
> у Яндекс Метрики — батч: Logs API отдаёт TSV-файл, и данных за сегодня в нём
> нет. Поток у Метрики есть только в платном тарифе «Метрика Про» и идёт не
> через Kafka, а через Yandex Data Transfer прямо в управляемый ClickHouse, с
> задержкой до 15 минут. Kafka в этой схеме не участвует.

Почему Kafka всё-таки уместна на стенде:

- Задачи, которые она ставит перед менти, реальные: чтение из потока,
  контроль смещений, дубли, опоздавшие события, обратное давление. Такие задачи
  есть в любом продуктовом дата-контуре — просто перед Kafka там стоит свой
  коллектор, а не Метрика.
- Механика «поток + версии одной записи» у Метрики Про (`Sign`, `HitVersion`,
  `VisitVersion`) ближе к потоковому приёму, чем к «скачал файл раз в сутки».
  Разговор про `ReplacingMergeTree` и `CollapsingMergeTree`, который стенд уже
  ведёт, попадает в реальную практику точно.

Чего делать нельзя: говорить менти «Яндекс отдаёт кликстрим в Kafka». Это
неправда.

## 3. AppMetrica

Источник: [ресурсы Logs
API](https://appmetrica.yandex.ru/docs/ru/mobile-api/logs/endpoints).

Главное отличие от Метрики: **AppMetrica отдаёт много узких таблиц вместо двух
широких.** Каждый вид данных — свой эндпоинт: `events`, `installations`,
`sessions_starts`, `ecommerce_events`, `revenue_events`, `ad_revenue_events`,
`crashes`, `errors`, `clicks`, `postbacks`, `deeplinks`, `push_tokens`,
`profiles_v2`. Формат — CSV или JSON.

Поле-строка с JSON **есть**: `event_json` — «атрибуты, сериализованные в JSON».
То есть у мобильного трекера Яндекса произвольные свойства события лежат
единым JSON в одной колонке — не так, как в Метрике с её массивами.

Метки времени у события — четыре, и это ровно то разделение, которого не
хватает Метрике:

| Поле | Что это |
|---|---|
| `event_datetime` | Время события, `yyyy-mm-dd hh:mm:ss` |
| `event_timestamp` | То же в unix-времени |
| `event_receive_datetime` | Время приёма на сервере (расходится с `event_datetime` из-за сети) |
| `event_receive_timestamp` | То же в unix-времени |

Идентификаторы: `appmetrica_device_id`, `installation_id`, `session_id`,
`profile_id`. Устройство и гео: `device_manufacturer`, `device_model`,
`device_type`, `os_name`, `os_version`, `city`, `country_iso_code`,
`google_aid`, `ios_ifa`, `ios_ifv`.

Потоковые возможности — Data Stream API (см. выше): пятиминутные окна,
задержка от 10 минут, хранение 7 дней, до 50 000 запросов в сутки против
5 000 в сутки у Logs API.

## 4. Ecommerce и выручка

### Как это описывается на сайте

Источник: [передача данных
ecommerce](https://yandex.ru/support/metrica/ru/ecommerce/data). Магазин кладёт
в `dataLayer` объект такой формы:

```javascript
{
  "ecommerce": {
    "currencyCode": "RUB",
    "purchase": {
      "actionField": { "id": "TRX987", "revenue": 12300, "coupon": "SALE10" },
      "products": [
        { "id": "SKU-1", "name": "Кружка", "price": 4100,
          "brand": "Acme", "category": "Посуда/Кружки",
          "variant": "белая", "quantity": 3 }
      ]
    }
  }
}
```

Виды действий: `impressions`, `click`, `detail`, `add`, `remove`, `purchase`,
`promoView`, `promoClick`. У покупки `actionField.id` обязателен; `revenue`
считается автоматически, если его не передать. У товара обязателен `id` или
`name`; остальное — `price`, `quantity`, `brand`, `category`, `variant`,
`coupon`, `list`, `position`, `discount`.

### Что из этого попадает в выгрузку

**Не JSON, а плоские массивы.** Один заказ на 3 товара превращается не в
вложенный объект, а в набор массивов одинаковой длины:

| Поле выгрузки | Тип | Что это |
|---|---|---|
| `ym:pv:purchaseID` | Array(String) | Идентификатор покупки |
| `ym:pv:purchaseRevenue` | Array(Float64) | **Сумма заказа** |
| `ym:pv:purchaseCurrency` | Array(String) | Валюта |
| `ym:pv:purchaseCoupon` | Array(String) | Промокод на весь заказ |
| `ym:pv:purchaseTax`, `purchaseShipping` | Array(String) | Налоги, доставка |
| `ym:pv:purchaseProductQuantity` | Array(UInt64) | Число товаров в покупке |
| `ym:pv:productID`, `productName`, `productBrand`, `productCategory` | Array(String) | Товар |
| `ym:pv:productPrice` | Array(Int64) | Цена товара |
| `ym:pv:productQuantity` | Array(UInt64) | Количество |
| `ym:pv:productEventType` | Array(String) | `view_item_list`, `click`, `detail`, `add`, `purchase`, `remove` |
| `ym:pv:ecommerce` | String | Сырое событие ecommerce (одна строка) |

Обратите внимание на две вещи. Первая: `purchaseTax` и `purchaseShipping` в
хитах имеют тип `Array(String)`, хотя это денежные суммы, — так в документации.
Вторая: рядом с разобранными массивами лежит поле `ym:pv:ecommerce` типа
`String`. То есть Яндекс отдаёт и разобранное, и сырое.

На уровне визита к этому добавляются `ym:s:purchaseDateTime`
`Array(DateTime)`, `ym:s:purchaseAffiliation`, весь блок
`ym:s:purchasedProduct*` (около 20 массивов про купленные товары) и
`ym:s:impressions*` (около 18 массивов про показы).

## 5. Как это кладут в ClickHouse

Здесь два первоисточника, и оба полезны.

### 5.1. Примерный датасет в документации ClickHouse

У ClickHouse корни в Метрике, и в его документации до сих пор лежит
обезличенный датасет Метрики: `hits_v1` (8 873 898 строк) и `visits_v1`
(1 680 609 строк). Источники:
[страница датасета](https://clickhouse.com/docs/getting-started/example-datasets/metrica),
DDL получен через MCP Context7 (`/clickhouse/clickhouse-docs`, файл
`docs/getting-started/example-datasets/anon_web_analytics_metrica.md`).

`hits_v1` — примерно 130 плоских колонок:

```sql
CREATE TABLE datasets.hits_v1
(
    WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16,
    EventTime DateTime, EventDate Date, CounterID UInt32,
    ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32,
    UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8,
    URL String, Referer String, URLDomain String, RefererDomain String,
    IsRobot UInt8, RefererCategories Array(UInt16), URLRegions Array(UInt32),
    ResolutionWidth UInt16, ResolutionHeight UInt16,
    ClientTimeZone Int16, ClientEventTime DateTime, UTCEventTime DateTime,
    Params String, GoalsReached Array(UInt32),
    UTMSource String, UTMMedium String, UTMCampaign String,
    UTMContent String, UTMTerm String, FromTag String,
    HasGCLID UInt8, RefererHash UInt64, URLHash UInt64,
    CLID UInt32, YCLID UInt64,
    ParsedParams Nested(Key1 String, Key2 String, Key3 String,
                        Key4 String, Key5 String, ValueDouble Float64),
    ...
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
```

Что тут стоит заметить:

- **Три метки времени:** `EventTime`, `ClientEventTime`, `UTCEventTime`. Плюс
  `ClientTimeZone`. Во внутренней схеме разделение клиента и сервера есть, хотя
  в публичную выгрузку хитов оно не попадает.
- **Ключ сортировки** — `(CounterID, EventDate, intHash32(UserID))`: сначала
  сайт, потом дата, потом пользователь. Не идентификатор события. Это ключ под
  типовые запросы «по сайту за период», а не под точечный поиск.
- **Ключ семплирования** `intHash32(UserID)` — чтобы считать по 10% данных и
  получать корректные метрики по пользователям.
- **Nested** вместо параллельных массивов: `ParsedParams Nested(Key1 String,
  …, ValueDouble Float64)`. В ClickHouse `Nested` — это синтаксический сахар
  над теми же параллельными массивами: колонка `ParsedParams.Key1` физически и
  есть `Array(String)`.
- Есть `YCLID UInt64` и `CLID UInt32` — метки Яндекс Директа.
- Есть хеши (`URLHash`, `RefererHash`, `NormalizedRefererHash`) — внутренняя
  оптимизация Яндекса под быстрые сравнения.

`visits_v1` — около 190 колонок и другой движок:

```sql
CREATE TABLE datasets.visits_v1
(
    CounterID UInt32, StartDate Date, Sign Int8, IsNew UInt8,
    VisitID UInt64, UserID UInt64, StartTime DateTime, Duration UInt32,
    UTCStartTime DateTime, PageViews Int32, Hits Int32, IsBounce UInt8,
    ...
    Goals Nested(ID UInt32, Serial UInt32, EventTime DateTime,
                 Price Int64, OrderID String, CurrencyID UInt32),
    WatchIDs Array(UInt64),
    TraficSource Nested(ID Int8, SearchEngineID UInt16, AdvEngineID UInt8,
                        PlaceID UInt16, SocialSourceNetworkID UInt8,
                        Domain String, SearchPhrase String,
                        SocialSourcePage String),
    ParsedParams Nested(Key1 String, ..., ValueDouble Float64),
    Market Nested(Type UInt8, GoalID UInt32, OrderID String,
                  OrderPrice Int64, PP UInt32, ...,
                  GoodID String, GoodName String,
                  GoodQuantity Int32, GoodPrice Int64),
    ...
)
ENGINE = CollapsingMergeTree(Sign)
PARTITION BY toYYYYMM(StartDate)
ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
SAMPLE BY intHash32(UserID)
```

Здесь `CollapsingMergeTree(Sign)` — тот самый механизм версий визита, о котором
пишет справка Метрики Про. Ecommerce лежит в `Nested`-блоке `Market` с полями
`GoodID`, `GoodName`, `GoodQuantity`, `GoodPrice`.

### 5.2. Официальные схемы потоковой выгрузки

Списки колонок для выгрузки в свой ClickHouse: [поля
хитов](https://yandex.ru/support/metrica/ru/pro/hits), [поля
визитов](https://yandex.ru/support/metrica/ru/pro/visits).

Существенное: **в облачной выгрузке имена колонок не в стиле API, а в стиле
ClickHouse.** Не `ym:s:visitID`, а `VisitID`. Не `ym:pv:watchID`, а `WatchID`.
Префиксы `ym:pv:` и `ym:s:` — это про язык API, в таблице их нет.

Колонки хитов, которые видит инженер:

| Колонка | Тип |
|---|---|
| `WatchID` | UInt64 |
| `pageViewID` | UInt32 |
| `VisitID` | UInt64 (с 10.10.2025) |
| `CounterID` | UInt32 |
| `ClientID` | UInt64 |
| `CounterUserIDHash` | UInt64 |
| `EventDate` | Date |
| `UTCEventTime` | DateTime |
| `ClientTimeZone` | Int16 |
| `Sign` | Int8 — признак статуса записи в инкрементальном логе |
| `HitVersion` | UInt32 |
| `URL`, `Title` | String |
| `GoalsReached` | Array(UInt32) |
| `ecommerce` | String |

Колонок в хитовой выгрузке около 140, в визитной — около 200. В визитной есть
`Sign` (Int8) и `VisitVersion` (UInt32).

Прицельная проверка по хитовой выгрузке: колонок `EventTime`,
`ClientEventTime`, `LocalEventTime`, `UserID` и `UserIDHash` там **нет**.
Единственная метка времени — `UTCEventTime`, а часовой пояс клиента — отдельным
числом `ClientTimeZone`.

## 6. Сравнение с западными трекерами

**Snowplow — плоское ядро.** Обогащённое событие — строка TSV из 131 колонки.
Таблица `atomic.events` описана как «широкая», и «отдельные поля хранятся в
своих колонках». Самоописываемые события и сущности в BigQuery, Snowflake и
Databricks добавляются как дополнительные колонки той же таблицы; в Redshift —
отдельными таблицами со связью один-к-одному по `event_id`. Источник:
[введение в таблицу atomic
events](https://docs.snowplow.io/docs/fundamentals/warehouse-tables/).

**Segment — вложенный JSON.** Событие `track` — это JSON, где `properties` и
`context` — вложенные объекты. Верхний уровень: `anonymousId`, `userId`,
`event`, `properties`, `context`, `type`, `messageId`, `timestamp`,
`originalTimestamp`, `sentAt`, `receivedAt`, `integrations`. Источник:
[Spec: Track](https://www.twilio.com/docs/segment/connections/spec/track).

**Amplitude — вложенный JSON.** В Export API `event_properties`,
`user_properties`, `group_properties`, `groups`, `data` — словари. Метки
времени: `client_event_time`, `client_upload_time`, `event_time`,
`server_upload_time`, `server_received_time`, `processed_time`. Источник:
[Export API](https://amplitude.com/docs/apis/analytics/export).

**Итог сравнения.** Яндекс ближе к Snowplow: плоское широкое ядро, всё
переменное — в массивах. Разница в том, что Snowplow добавляет колонки под
каждую схему события, а Яндекс держит фиксированный набор массивов
(`parsedParamsKey1..10`, `product*`, `purchase*`) и отдельное сырое поле
(`params`, `ecommerce`). До Segment и Amplitude, где `properties` — свободный
JSON-объект, Яндексу далеко.

## Вывод: что копировать стенду

### Форма

**Плоское широкое событие с массивами.** Одно событие = одна строка. Внутри —
плоские колонки. Всё, что бывает «много раз внутри одного события», — набор
параллельных массивов одной длины (или `Nested`, что в ClickHouse то же самое).
Свободного вложенного JSON вроде `context` или `properties` не делать: в РФ
менти его не встретит.

Стенду стоит воспроизводить именно **хит** — одно действие с полным контекстом.
Визит оставить тем, чем он сейчас является: результатом сборки в DDS/DM. Так
менти сам делает то, что Метрика делает за него, и понимает, откуда берётся
`visitDuration` и `pageViews`.

Отдельно рекомендуется добавить **одно сырое поле-строку с JSON** — как
`ym:pv:ecommerce` у Метрики и `event_json` у AppMetrica. Это даёт честное
упражнение «разобрать JSON внутри колонки», которое в бою встречается постоянно.

### Список полей для широкого события стенда

Колонка «есть» — про текущий стенд (`sql/ddl/ods/20_ods.sql`,
`sql/ddl/dds/30_dds.sql`). «нет» означает: поля у нас сейчас нет.

| № | Имя | Тип | Откуда взято | Есть у нас |
|---|---|---|---|---|
| 1 | `WatchID` | UInt64 | `ym:pv:watchID`, `hits_v1.WatchID` | ~ (есть `event_id` UUID, тип другой) |
| 2 | `VisitID` | UInt64 | `ym:pv:visitID`, `visits_v1.VisitID` | ~ (есть `click_id` UUID, тип другой) |
| 3 | `pageViewID` | UInt32 | `ym:pv:pageViewID` | **нет** |
| 4 | `CounterID` | UInt32 | `ym:pv:counterID` | **нет** |
| 5 | `ClientID` | UInt64 | `ym:pv:clientID` — анонимный id браузера | **нет** ← важное |
| 6 | `CounterUserIDHash` | UInt64 | `ym:pv:counterUserIDHash` | **нет** |
| 7 | `UTCEventTime` | DateTime | `Метрика Про, хиты` | ~ (есть `event_ts`, одна метка) |
| 8 | `EventDate` | Date | `Метрика Про, хиты` | есть (`event_date`) |
| 9 | `ClientTimeZone` | Int16 (минуты) | `ym:pv:clientTimeZone` | **нет** ← важное |
| 10 | `ClientEventTime` | DateTime | `hits_v1.ClientEventTime`; у AppMetrica — `event_datetime` против `event_receive_datetime` | **нет** ← важное |
| 11 | `URL` | String | `ym:pv:URL` | есть (`page_url`) |
| 12 | `Title` | String | `ym:pv:title` | **нет** |
| 13 | `Referer` | String | `ym:pv:referer` | есть (`referer_url`) |
| 14 | `UTMSource` | String | `ym:pv:UTMSource` | есть |
| 15 | `UTMMedium` | String | `ym:pv:UTMMedium` | есть |
| 16 | `UTMCampaign` | String | `ym:pv:UTMCampaign` | есть |
| 17 | `UTMContent` | String | `ym:pv:UTMContent` | есть |
| 18 | `UTMTerm` | String | `ym:pv:UTMTerm` | **нет** |
| 19 | `LastTrafficSource` | String | `ym:pv:lastTrafficSource` | ~ (есть `referer_medium`, смысл близкий) |
| 20 | `LastSearchEngineRoot` | String | `ym:pv:lastSearchEngineRoot` | **нет** |
| 21 | `HasGCLID` | UInt8 | `ym:pv:hasGCLID` | **нет** |
| 22 | `YCLID` | UInt64 | `hits_v1.YCLID` (в Logs API не найден) | **нет** |
| 23 | `Browser` | String | `ym:pv:browser` | есть (`browser_name`) |
| 24 | `BrowserMajorVersion` | UInt16 | `ym:pv:browserMajorVersion` | **нет** |
| 25 | `BrowserLanguage` | String | `ym:pv:browserLanguage` | есть (`browser_language`) |
| 26 | `OperatingSystem` | String | `ym:pv:operatingSystem` | есть (`os`) |
| 27 | `OperatingSystemRoot` | String | `ym:pv:operatingSystemRoot` | есть (`os_name`) |
| 28 | `DeviceCategory` | String (1..4) | `ym:pv:deviceCategory` | есть (`device_type`) |
| 29 | `MobilePhoneModel` | String | `ym:pv:mobilePhoneModel` | **нет** |
| 30 | `ScreenWidth`, `ScreenHeight` | UInt16 | `ym:pv:screenWidth/Height` | **нет** |
| 31 | `IPAddress` | String | `ym:pv:ipAddress` | есть (`ip_address`) |
| 32 | `RegionCountry` | String (ISO) | `ym:pv:regionCountry` | есть (`geo_country`) |
| 33 | `RegionCity` | String | `ym:pv:regionCity` | ~ (есть `geo_region_name`) |
| 34 | `RegionCountryID`, `RegionCityID` | UInt32 | `ym:pv:regionCountryID/CityID` | **нет** ← важное |
| 35 | `IsPageView` | UInt8 | `ym:pv:isPageView` | **нет** |
| 36 | `NotBounce` | UInt8 | `ym:pv:notBounce` | **нет** |
| 37 | `HTTPError` | String | `ym:pv:httpError` | **нет** |
| 38 | `GoalsReached` | Array(UInt32) | `ym:pv:goalsID`, `hits_v1.GoalsReached` | **нет** ← ключевое |
| 39 | `ParsedParams` | Nested(Key1..Key5 String, ValueDouble Float64) | `hits_v1.ParsedParams`; в Logs API — `parsedParamsKey1..10` | **нет** ← ключевое |
| 40 | `Params` | String (сырой JSON) | `ym:pv:params` | **нет** |
| 41 | `ecommerce` | String (сырой JSON) | `ym:pv:ecommerce`; у AppMetrica — `event_json` | **нет** ← ключевое |
| 42 | `purchaseID` | Array(String) | `ym:pv:purchaseID` | **нет** ← ключевое |
| 43 | `purchaseRevenue` | Array(Float64) | `ym:pv:purchaseRevenue` — **выручка** | **нет** ← ключевое |
| 44 | `purchaseCurrency` | Array(String) | `ym:pv:purchaseCurrency` | **нет** |
| 45 | `purchaseCoupon` | Array(String) | `ym:pv:purchaseCoupon` | **нет** |
| 46 | `productID` | Array(String) | `ym:pv:productID` | **нет** ← ключевое |
| 47 | `productName` | Array(String) | `ym:pv:productName` | **нет** |
| 48 | `productCategory` | Array(String) | `ym:pv:productCategory` | **нет** |
| 49 | `productPrice` | Array(Int64) | `ym:pv:productPrice` | **нет** ← ключевое |
| 50 | `productQuantity` | Array(UInt64) | `ym:pv:productQuantity` | **нет** ← ключевое |
| 51 | `productEventType` | Array(String) | `ym:pv:productEventType`: `detail`, `add`, `remove`, `purchase`… | **нет** ← ключевое |
| 52 | `Sign` | Int8 | Метрика Про, хиты и визиты | **нет** ← ключевое |
| 53 | `HitVersion` | UInt32 | Метрика Про, хиты | **нет** |

Поля 38–53 — то, чего стенду не хватает сильнее всего: цели, свои параметры,
ecommerce с выручкой и механика версий записи. Ровно про это спрашивают на
работе в первую очередь, и ровно этого сейчас у нас нет
(см. `docs/generator-realism.md`, раздел «Что упрощено»).

Ключ сортировки для широкой таблицы стенда стоит взять по образцу Метрики:
`ORDER BY (CounterID, EventDate, intHash32(ClientID))`, а не по идентификатору
события. Это заодно повод объяснить менти, зачем ключ сортировки строится под
запросы, а не под уникальность.

### Чего воспроизводить не стоит

1. **Полный набор полей.** 140 колонок в хитах и 200 в визитах — это шум.
   Учебной ценности в 56 массивах нет никакой, а поддерживать их дорого.
   Хватит 40–50 колонок из таблицы выше.
2. **Отдельную сущность «визит» из Яндекса.** Если брать и хиты, и визиты,
   стенд потеряет главное упражнение — сборку сессий своими руками. Берём
   только хиты.
3. **Полную механику `CollapsingMergeTree` с пересчётом визита.** Колонку
   `Sign` добавить полезно — это узнаваемо и объясняет `sum(Sign)`. А вот
   пересчитывать визит и присылать по нему пять версий — сложность, которая
   съест урок целиком. Достаточно показать `Sign` на хитах.
4. **Устаревшие технологии из датасета ClickHouse.** `FlashMajor`,
   `SilverlightVersion1..4`, `NetMajor` — следы 2013 года. Живой аналог
   сегодня им не соответствует, копировать нечего.
5. **Хеши** (`URLHash`, `RefererHash`, `NormalizedStartURLHash`). Это
   внутренняя оптимизация Яндекса. Менти без контекста примет их за содержимое.
6. **Социально-демографические поля** (`Age`, `Sex`, `Income`, `Interests`,
   `GeneralInterests`, `Robotness`). Это внутренние оценки Яндекса, вне Яндекса
   их не получить. Ставить их в генератор — учить менти работать с данными,
   которых у него не будет.
7. **Openstat и поля Яндекс Директа** (`openstatAd`, `openstatCampaign`,
   `DirectClickOrder`, `DirectBannerGroup`, `DirectPhraseOrCond` и десяток
   соседних). Openstat — устаревший стандарт метки трафика. Поля Директа
   осмысленны только при связке аккаунтов. Ставим одну метку `YCLID` и одну
   `HasGCLID` — этого хватит для разговора об атрибуции.
8. **Пять уровней категорий товара** (`productCategoryLevel1..5`) и блоки
   `purchasedProduct*` (~20 массивов), `impressions*` (~18 массивов),
   `promotion*`, `offlineCall*`. Механика та же, что у `product*`; повторять её
   четыре раза — только объём.
9. **Префиксы `ym:pv:` и `ym:s:` в именах колонок.** Это язык API, а не имена
   в базе. Сам Яндекс в облачной выгрузке их не использует.
10. **Смену UUID на UInt64 у наших `event_id` и `click_id`.** У Яндекса
    идентификаторы числовые, но переделывать под это весь стенд — работа без
    учебной отдачи. Достаточно добавить `ClientID` типа UInt64 — это самое
    узнаваемое поле Метрики, и его отсутствие у нас заметнее всего.
11. **`browser_user_agent` как «поле Метрики».** У Яндекса строки user agent в
    выгрузке нет — отдаются уже разобранные поля. Само поле на стенде оставить
    можно (разбор user agent — реальная задача), но нельзя выдавать его за
    формат Яндекса.
12. **`geo_latitude` и `geo_longitude` как «поля Метрики».** Координат в
    выгрузке Метрики нет. Если они нужны для карт в Superset, надо честно
    сказать, что это наша добавка.

## Что осталось неподтверждённым

- Поле `UserID` (свой идентификатор пользователя со стороны сайта) в списках
  выгружаемых полей Logs API и в облачной выгрузке хитов найти не удалось.
- Поле `yclid` в Logs API не найдено. Колонка `YCLID UInt64` есть в примерном
  датасете ClickHouse.
- Ограничение «не более 1 ГБ на один файл выгрузки» встречается в поиске по
  документации Метрики, но дословно на официальной странице не подтверждено.
- Сколько времени хранится подготовленный лог до удаления — на изученных
  страницах не сказано; сказано только, что удалять их надо самому, иначе
  кончится квота 10 ГБ.
- Экспорт кликстрима Метрики в Yandex Object Storage или в Yandex Data Streams
  (сервис с Kafka-совместимым интерфейсом) официальной документацией не
  подтверждён. Единственный подтверждённый потоковый приёмник — управляемый
  ClickHouse через Yandex Data Transfer.
- Точное число колонок в облачной выгрузке (около 140 для хитов, около 200 для
  визитов) — оценка по объёму страниц, а не цифра из документации.

## Источники

Яндекс Метрика, Logs API:

- [Введение в Logs API](https://yandex.ru/dev/metrika/ru/logs/)
- [Поля хитов](https://yandex.ru/dev/metrika/ru/logs/fields/hits)
- [Поля визитов](https://yandex.ru/dev/metrika/ru/logs/fields/visits)
- [Блог Метрики: выгружайте сырые данные через Logs API](https://yandex.ru/blog/metrika/vygruzhayte-syrye-dannye-iz-metriki-cherez-logs-api)
- [setUserID](https://yandex.com/support/metrica/en/objects/set-user-id.html)

Яндекс Метрика, выгрузка в облако:

- [Data Streaming (интеграция с Yandex Cloud)](https://yandex.ru/support/metrica/ru/uploading-data/cloud)
- [Метрика Про: как работать с данными](https://yandex.ru/support/metrica/ru/pro/data-work)
- [Метрика Про: поля хитов](https://yandex.ru/support/metrica/ru/pro/hits)
- [Метрика Про: поля визитов](https://yandex.ru/support/metrica/ru/pro/visits)
- [Yandex Cloud: репликация данных Метрики в ClickHouse](https://yandex.cloud/ru/docs/tutorials/dataplatform/metrika-to-clickhouse)

Ecommerce:

- [Передача данных ecommerce](https://yandex.ru/support/metrica/ru/ecommerce/data)

AppMetrica:

- [Logs API: ресурсы и поля](https://appmetrica.yandex.ru/docs/ru/mobile-api/logs/endpoints)
- [Data Stream API: описание](https://appmetrica.yandex.ru/docs/ru/mobile-api/datastream/about)

ClickHouse:

- [Примерный датасет Метрики](https://clickhouse.com/docs/getting-started/example-datasets/metrica)
- DDL таблиц `hits_v1` и `visits_v1` получен через MCP Context7
  (`/clickhouse/clickhouse-docs`, файл
  `docs/getting-started/example-datasets/anon_web_analytics_metrica.md`)

Западные трекеры:

- [Snowplow: введение в таблицу atomic events](https://docs.snowplow.io/docs/fundamentals/warehouse-tables/)
- [Segment Spec: Track](https://www.twilio.com/docs/segment/connections/spec/track)
- [Amplitude Export API](https://amplitude.com/docs/apis/analytics/export)
