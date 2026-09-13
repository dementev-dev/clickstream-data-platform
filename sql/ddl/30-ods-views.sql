-- Актуальные события: FINAL выбирает последнюю доставку по _load_ts,
-- не дожидаясь фоновых слияний. Поля и типы остаются как в ods.event_dist.

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

-- Разбор STG в события и ошибки. Два представления используют одинаковое
-- условие годности; второе берет его отрицание. Условие дает только 0 или 1:
-- NULL не выбрала бы ни одна ветвь. Функции разбора не вызывают исключений
-- на некорректном raw, чтобы одно сообщение не прервало прием.
--
-- Оба представления срабатывают на вставку в stg.hits_raw_dist,
-- до распределения по шардам. Порядок SELECT совпадает с целевой таблицей.
--
-- Прием проверяет объект JSON, точный набор ключей и разбор пяти полей:
-- WatchID, VisitID, ClientID, EventDate, UTCEventTime. Остальные поля
-- извлекаются по формату без отдельной проверки. Граница — ADR 0005.
-- arraySort с обеих сторон позволяет сравнить ключи независимо от порядка.
--
-- UTCEventTime приходит строкой вида "2026-06-01T12:34:56Z". Ее разбирает
-- parseDateTimeOrNull с явной маской: JSONExtract в DateTime такую форму
-- не принимает, а BestEffort допускает неполные даты. Пояс UTC указан
-- отдельно, потому что Z в маске проверяется как буква. EventDate приходит
-- как "2026-06-01" и разбирается через JSONExtract.

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
    -- purchaseRevenue приходит числом рублей и разбирается сразу в Decimal.
    -- Прямой разбор сверяли через Context7 и на ClickHouse 26.3 (#155).
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
    -- _load_ts остается временем исходной доставки и версией события.
    _load_ts
FROM stg.hits_raw_dist
WHERE is_object AND keys_match AND key_fields_parsed;

-- Ошибки: отрицание того же условия годности.
-- Класс определяется первым нарушенным условием. Например, JSON-строка
-- не проходит и проверку объекта, и проверку ключей, но получает not_an_object.
-- Условие повторено рядом с запросом, без отдельной функции ClickHouse.

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
