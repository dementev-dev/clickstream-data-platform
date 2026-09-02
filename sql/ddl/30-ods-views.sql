-- ODS: каноническое чтение актуальных событий.
--
-- Представление сохраняет поля и зерно ods.event_dist, но скрывает FINAL от
-- потребителя. Поэтому физическая таблица остаётся местом диагностики
-- доставленных версий, а точное чтение получает одно имя.
CREATE VIEW IF NOT EXISTS ods.event_v ON CLUSTER clickstream_cluster
AS
SELECT *
FROM ods.event_dist FINAL;

-- ODS: каноническое чтение текущего состояния заказов.
--
-- То же, что у событий, но версию здесь задаёт источник: физическая таблица
-- хранит все приехавшие версии заказа, а FINAL оставляет приехавшую
-- позднейшим слепком — колонка версии здесь snapshot_date. Читать заказы
-- голым SELECT по _dist значит зависеть от того, сколько фоновых слияний
-- успело пройти.
--
-- Поля остаются языком источника, обогащения нет: DDS читает это
-- представление и отдельно решает, какой будет его модель (ADR 0010).
-- Способ выбора версии можно заменить, не трогая потребителей, — за именем
-- скрыт FINAL, а не устройство.
CREATE VIEW IF NOT EXISTS ods.order_v ON CLUSTER clickstream_cluster
AS
SELECT *
FROM ods.order_dist FINAL;

-- ODS: разбор STG в событие и в таблицу ошибок.
--
-- Матвью две, и вместе они обязаны делить поток без зазора и без нахлёста:
-- строка, подошедшая обеим, задвоится, а не подошедшая ни одной — исчезнет
-- молча. Держится это формой: годность считает один и тот же предикат из трёх
-- частей, а вторая матвью берёт его буквальное отрицание.
--
-- Отсюда два запрета на выражения предиката, и оба серьёзные.
--
-- Первый: ни одна его часть не возвращает NULL. Трёхзначная логика дала бы
-- строку, которую не берёт ни «условие», ни «NOT условие», — и потерялась бы
-- она бесшумно. Поэтому сравнения здесь дают 0 или 1, а разбор в Nullable
-- заканчивается IS NOT NULL.
--
-- Второй: ни одна его часть не бросает исключений. Упавшая матвью роняет
-- вставку, офсеты не коммитятся, и потребление топика встаёт до починки —
-- это единственное, на чём сейчас держится правило репозитория «грязные
-- записи не валят пайплайн». Поэтому приведение Nullable к необнуляемому типу
-- (assumeNotNull) стоит только в списке колонок, за предикатом, который NULL
-- уже отсёк, а в самом предикате живут только функции семейства
-- JSONExtract/*OrNull. Довод целиком — ADR 0005.
--
-- Источник у обеих — stg.hits_raw_dist, лицо слоя, а не локальная таблица:
-- потребитель цепляется к лицу, а матвью с источником-Distributed срабатывает
-- на вставку именно в распределённую таблицу, до раскладки по шардам.
--
-- Порядок колонок в SELECT совпадает с порядком в целевой таблице.

-- Годное событие: строка, прошедшая строгий приём.
--
-- Строгий приём — это три вопроса: объект ли это JSON, тот ли набор ключей,
-- разбираются ли в свой тип пять опорных колонок. Почему именно так — почему
-- объект, а не валидность; почему сверка ключей заменяет сорок семь проверок
-- на присутствие; почему опорных пять, а не сорок семь — ADR 0005, «Решение».
--
-- arraySort ниже стоит с обеих сторон, и убирать его нельзя: без обёртки сорок
-- семь CamelCase-имён пришлось бы держать ровно в байтовом порядке, а сбой
-- порядка увёл бы в брак вообще всё, и молча (ADR 0005).
--
-- Метка времени — единственная из пяти, кого разбирает не JSONExtract, и это
-- измеренная необходимость, а не вкус. На проводе UTCEventTime уезжает в
-- ISO-8601 с суффиксом зоны — «2026-06-01T12:34:56Z» (спека генератора,
-- раздел 4), а JSONExtract с типом DateTime такую строку не берёт и отдаёт
-- NULL. Оставь его здесь — и в брак уехали бы все события до единого.
--
-- Формат назван буквально, а не отдан parseDateTimeBestEffort, и вот почему.
-- Best-effort понимает десяток записей и на непонятной не краснеет, а
-- достраивает недостающее: обрезанное «20:00:21» он превращает в первое
-- января текущего года. Такая строка прошла бы строгий приём с тихо неверным
-- временем — ровно с той порчей, ради которой класс key_field_unparsed и
-- заведён. Источник у топика один и шлёт одну запись, так что широта здесь не
-- нужна вовсе, а стоит она отключённой проверкой. Замеры — ADR 0005,
-- «Что проверено».
--
-- Третьим аргументом назван пояс — 'UTC'. Суффикс Z маска сверяет как букву и
-- выбрасывает, зоны из строки не берёт вовсе, поэтому без имени функция читала
-- бы показания часов по поясу сессии, а тот по умолчанию серверный. Тип
-- колонки этого не чинит: разбор отдаёт готовое число, и колонка кладёт его
-- как есть — пояс приёмника решал бы судьбу строки, а не числа.
-- Правило и замер — docs/architecture/storage.md, «Часовые пояса».
--
-- EventDate в такой подпорке не нуждается: дата уезжает как «2026-06-01», и
-- JSONExtract её берёт.

CREATE MATERIALIZED VIEW IF NOT EXISTS ods.event_mv ON CLUSTER clickstream_cluster
TO ods.event_dist
AS
WITH
    JSONType(raw) = 'Object' AS is_object,
    arraySort(JSONExtractKeys(raw)) = arraySort([
        'WatchID', 'VisitID', 'ClientID', 'CounterID', 'EventDate',
        'UTCEventTime', 'ClientTimeZone', 'EventType', 'Sign',
        'URL', 'Referer', 'Title', 'UTMSource', 'UTMMedium', 'UTMCampaign',
        'UTMContent', 'UTMTerm', 'LastTrafficSource', 'HasGCLID', 'YCLID',
        'Browser', 'BrowserMajorVersion', 'BrowserLanguage', 'OperatingSystem',
        'OperatingSystemRoot', 'DeviceCategory', 'MobilePhoneModel',
        'ScreenWidth', 'ScreenHeight', 'IPAddress', 'RegionCountry',
        'RegionCity', 'RegionCountryID', 'RegionCityID',
        'GoalsReached', 'ParsedParamsKey1',
        'purchaseID', 'purchaseRevenue', 'purchaseCurrency', 'purchaseCoupon',
        'productID', 'productName', 'productCategory', 'productPrice',
        'productQuantity', 'productEventType', 'ecommerce'
    ]) AS keys_match,
    JSONExtract(raw, 'WatchID', 'Nullable(UInt64)') IS NOT NULL
        AND JSONExtract(raw, 'VisitID', 'Nullable(UInt64)') IS NOT NULL
        AND JSONExtract(raw, 'ClientID', 'Nullable(UInt64)') IS NOT NULL
        AND JSONExtract(raw, 'EventDate', 'Nullable(Date)') IS NOT NULL
        AND parseDateTimeOrNull(JSONExtractString(raw, 'UTCEventTime'),
            '%Y-%m-%dT%H:%i:%SZ', 'UTC') IS NOT NULL AS key_fields_parsed
SELECT
    JSONExtract(raw, 'WatchID', 'UInt64') AS WatchID,
    JSONExtract(raw, 'VisitID', 'UInt64') AS VisitID,
    JSONExtract(raw, 'ClientID', 'UInt64') AS ClientID,
    JSONExtract(raw, 'CounterID', 'UInt32') AS CounterID,
    JSONExtract(raw, 'EventDate', 'Date') AS EventDate,
    assumeNotNull(parseDateTimeOrNull(
        JSONExtractString(raw, 'UTCEventTime'),
        '%Y-%m-%dT%H:%i:%SZ', 'UTC')) AS UTCEventTime,
    JSONExtract(raw, 'ClientTimeZone', 'Int16') AS ClientTimeZone,
    JSONExtract(raw, 'EventType', 'String') AS EventType,
    JSONExtract(raw, 'Sign', 'Int8') AS Sign,
    JSONExtract(raw, 'URL', 'String') AS URL,
    JSONExtract(raw, 'Referer', 'String') AS Referer,
    JSONExtract(raw, 'Title', 'String') AS Title,
    JSONExtract(raw, 'UTMSource', 'String') AS UTMSource,
    JSONExtract(raw, 'UTMMedium', 'String') AS UTMMedium,
    JSONExtract(raw, 'UTMCampaign', 'String') AS UTMCampaign,
    JSONExtract(raw, 'UTMContent', 'String') AS UTMContent,
    JSONExtract(raw, 'UTMTerm', 'String') AS UTMTerm,
    JSONExtract(raw, 'LastTrafficSource', 'String') AS LastTrafficSource,
    JSONExtract(raw, 'HasGCLID', 'UInt8') AS HasGCLID,
    JSONExtract(raw, 'YCLID', 'UInt64') AS YCLID,
    JSONExtract(raw, 'Browser', 'String') AS Browser,
    JSONExtract(raw, 'BrowserMajorVersion', 'UInt16') AS BrowserMajorVersion,
    JSONExtract(raw, 'BrowserLanguage', 'String') AS BrowserLanguage,
    JSONExtract(raw, 'OperatingSystem', 'String') AS OperatingSystem,
    JSONExtract(raw, 'OperatingSystemRoot', 'String') AS OperatingSystemRoot,
    JSONExtract(raw, 'DeviceCategory', 'UInt8') AS DeviceCategory,
    JSONExtract(raw, 'MobilePhoneModel', 'String') AS MobilePhoneModel,
    JSONExtract(raw, 'ScreenWidth', 'UInt16') AS ScreenWidth,
    JSONExtract(raw, 'ScreenHeight', 'UInt16') AS ScreenHeight,
    JSONExtract(raw, 'IPAddress', 'String') AS IPAddress,
    JSONExtract(raw, 'RegionCountry', 'String') AS RegionCountry,
    JSONExtract(raw, 'RegionCity', 'String') AS RegionCity,
    JSONExtract(raw, 'RegionCountryID', 'UInt32') AS RegionCountryID,
    JSONExtract(raw, 'RegionCityID', 'UInt32') AS RegionCityID,
    JSONExtract(raw, 'GoalsReached', 'Array(UInt32)') AS GoalsReached,
    JSONExtract(raw, 'ParsedParamsKey1', 'Array(String)') AS ParsedParamsKey1,
    JSONExtract(raw, 'purchaseID', 'Array(String)') AS purchaseID,
    -- Выгрузка несет JSON-числа типа Float64, но деньги хранятся в Decimal.
    -- JSONExtract разбирает их сразу, без промежуточного двоичного числа:
    -- синтаксис подтвержден документацией ClickHouse через Context7 и
    -- запросом к стендовому ClickHouse 26.3 при исполнении #155.
    JSONExtract(raw, 'purchaseRevenue', 'Array(Decimal(18, 2))') AS purchaseRevenue,
    JSONExtract(raw, 'purchaseCurrency', 'Array(String)') AS purchaseCurrency,
    JSONExtract(raw, 'purchaseCoupon', 'Array(String)') AS purchaseCoupon,
    JSONExtract(raw, 'productID', 'Array(String)') AS productID,
    JSONExtract(raw, 'productName', 'Array(String)') AS productName,
    JSONExtract(raw, 'productCategory', 'Array(String)') AS productCategory,
    JSONExtract(raw, 'productPrice', 'Array(Int64)') AS productPrice,
    JSONExtract(raw, 'productQuantity', 'Array(UInt64)') AS productQuantity,
    JSONExtract(raw, 'productEventType', 'Array(String)') AS productEventType,
    JSONExtract(raw, 'ecommerce', 'String') AS ecommerce,
    -- Метка загрузки переносится из STG как есть, а не ставится заново:
    -- она отвечает на вопрос «когда строка приехала в хранилище». Поставь
    -- здесь now64(3) — и колонка версии молча ответила бы на другой вопрос.
    _load_ts
FROM stg.hits_raw_dist
WHERE is_object AND keys_match AND key_fields_parsed;

-- Брак: буквальное отрицание того же предиката.
--
-- Классы брака пересекаются, поэтому проверяются по порядку, а в error_class
-- пишется первый совпавший. Скаляр проваливает и проверку на объект, и сверку
-- ключей — JSONExtractKeys от него даёт пустой массив (измерено на стенде
-- 7 августа 2026 года), — и без объявленного порядка попал бы то в один
-- класс, то в другой.
--
-- Предикат повторён здесь дословно, и это выбор, а не безвыходность: назвать
-- его один раз на две матвью позволил бы CREATE FUNCTION. Отвергнуто — условие
-- разбора ушло бы за имя, в отдельный объект со своей жизнью, и слой перестал
-- бы читаться по своему же DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS ods.event_errors_mv ON CLUSTER clickstream_cluster
TO ods.event_errors_dist
AS
WITH
    JSONType(raw) = 'Object' AS is_object,
    arraySort(JSONExtractKeys(raw)) = arraySort([
        'WatchID', 'VisitID', 'ClientID', 'CounterID', 'EventDate',
        'UTCEventTime', 'ClientTimeZone', 'EventType', 'Sign',
        'URL', 'Referer', 'Title', 'UTMSource', 'UTMMedium', 'UTMCampaign',
        'UTMContent', 'UTMTerm', 'LastTrafficSource', 'HasGCLID', 'YCLID',
        'Browser', 'BrowserMajorVersion', 'BrowserLanguage', 'OperatingSystem',
        'OperatingSystemRoot', 'DeviceCategory', 'MobilePhoneModel',
        'ScreenWidth', 'ScreenHeight', 'IPAddress', 'RegionCountry',
        'RegionCity', 'RegionCountryID', 'RegionCityID',
        'GoalsReached', 'ParsedParamsKey1',
        'purchaseID', 'purchaseRevenue', 'purchaseCurrency', 'purchaseCoupon',
        'productID', 'productName', 'productCategory', 'productPrice',
        'productQuantity', 'productEventType', 'ecommerce'
    ]) AS keys_match,
    JSONExtract(raw, 'WatchID', 'Nullable(UInt64)') IS NOT NULL
        AND JSONExtract(raw, 'VisitID', 'Nullable(UInt64)') IS NOT NULL
        AND JSONExtract(raw, 'ClientID', 'Nullable(UInt64)') IS NOT NULL
        AND JSONExtract(raw, 'EventDate', 'Nullable(Date)') IS NOT NULL
        AND parseDateTimeOrNull(JSONExtractString(raw, 'UTCEventTime'),
            '%Y-%m-%dT%H:%i:%SZ', 'UTC') IS NOT NULL AS key_fields_parsed
SELECT
    raw,
    multiIf(
        NOT is_object, 'not_an_object',
        NOT keys_match, 'keyset_mismatch',
        'key_field_unparsed'
    ) AS error_class,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    consumer_host,
    _load_ts
FROM stg.hits_raw_dist
WHERE NOT (is_object AND keys_match AND key_fields_parsed);
