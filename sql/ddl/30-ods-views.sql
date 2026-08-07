-- ODS: разбор сырья в событие и в таблицу ошибок.
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
-- Строгий приём — это три вопроса, и первые два держат весь контракт схемы.
--
-- 1. Это вообще объект JSON? Проверяется именно объект, а не валидность:
--    isValidJSON('123') возвращает единицу — скаляр тоже законный JSON
--    (измерено на стенде 7 августа 2026 года).
--
-- 2. Совпадает ли набор ключей с контрактным — все сорок семь имён, ни одного
--    лишнего. Одно это сравнение заменяет сорок семь проверок на присутствие
--    и ловит то, чего иначе не поймать вовсе: опечатку в имени поля (для
--    хранилища это одновременно пропавшее ожидаемое и появившееся лишнее),
--    молчаливое расширение контракта источником и любую подмену имени в
--    колонке-массиве. Обязательны все сорок семь: генератор шлёт их в каждом
--    событии, а «пусто» по контракту — пустое значение, а не отсутствие
--    ключа.
--
--    arraySort стоит с обеих сторон, и это не украшение. Без него сорок семь
--    CamelCase-имён пришлось бы выписать руками ровно в байтовом порядке —
--    ошибка, которая увела бы в брак вообще всё, и притом молча.
--
-- 3. Разбираются ли пять опорных колонок в свой тип. Не сорок семь, а пять:
--    идентификаторы события, визита и посетителя, дата партиции и метка
--    времени. Порча любой из них отравляет всё ниже по течению, тогда как
--    единственный производитель топика — свой генератор, сериализующий по
--    объявленным типам, и неверный тип может прийти только из руки. Сорок
--    семь проверок на NULL превратили бы матвью в простыню, не добавив
--    защиты. Присутствие остальных сорока двух держит вопрос 2.
--
-- Метку времени разбирает не JSONExtract, а parseDateTimeBestEffortOrNull, и
-- это измеренная необходимость, а не вкус. На проводе UTCEventTime уезжает в
-- ISO-8601 с суффиксом зоны — «2026-06-01T12:34:56Z» (спека генератора,
-- раздел 4), а JSONExtract с типом DateTime такую строку не берёт и отдаёт
-- NULL. Оставь его здесь — и в брак уехали бы все события до единого. Замер и
-- его подробности — ADR 0005, «Что проверено». EventDate в такой подпорке не
-- нуждается: дата уезжает как «2026-06-01», и JSONExtract её берёт.

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
        AND parseDateTimeBestEffortOrNull(JSONExtractString(raw, 'UTCEventTime'))
            IS NOT NULL AS key_fields_parsed
SELECT
    JSONExtract(raw, 'WatchID', 'UInt64') AS WatchID,
    JSONExtract(raw, 'VisitID', 'UInt64') AS VisitID,
    JSONExtract(raw, 'ClientID', 'UInt64') AS ClientID,
    JSONExtract(raw, 'CounterID', 'UInt32') AS CounterID,
    JSONExtract(raw, 'EventDate', 'Date') AS EventDate,
    assumeNotNull(parseDateTimeBestEffortOrNull(
        JSONExtractString(raw, 'UTCEventTime'))) AS UTCEventTime,
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
    JSONExtract(raw, 'purchaseRevenue', 'Array(Float64)') AS purchaseRevenue,
    JSONExtract(raw, 'purchaseCurrency', 'Array(String)') AS purchaseCurrency,
    JSONExtract(raw, 'purchaseCoupon', 'Array(String)') AS purchaseCoupon,
    JSONExtract(raw, 'productID', 'Array(String)') AS productID,
    JSONExtract(raw, 'productName', 'Array(String)') AS productName,
    JSONExtract(raw, 'productCategory', 'Array(String)') AS productCategory,
    JSONExtract(raw, 'productPrice', 'Array(Int64)') AS productPrice,
    JSONExtract(raw, 'productQuantity', 'Array(UInt64)') AS productQuantity,
    JSONExtract(raw, 'productEventType', 'Array(String)') AS productEventType,
    JSONExtract(raw, 'ecommerce', 'String') AS ecommerce,
    -- Метка загрузки переносится из сырья как есть, а не ставится заново:
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
-- бы читаться по своему же DDL. Расхождения двух копий сторожит соседство:
-- обе живут в одном файле, в полусотне строк друг от друга.
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
        AND parseDateTimeBestEffortOrNull(JSONExtractString(raw, 'UTCEventTime'))
            IS NOT NULL AS key_fields_parsed
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
