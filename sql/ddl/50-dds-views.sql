-- DDS: публичный договор события на языке бизнес-модели.
--
-- Событие не копируется в DDS: обычное представление вторым уровнем читает
-- точное состояние из ods.event_v. Явный список колонок не пропускает в
-- договор служебные и сырые поля ODS при будущих изменениях источника.
--
-- Представление — договор чтения, не граница прав: роль bi_reader читает его
-- источник по решению docs/adr/0007-clickhouse-access.md.
--
-- event_date берётся напрямую из EventDate. Пересчёт из event_time помешал бы
-- первичному индексу отбирать день до чтения.
--
-- Europe/Samara задаёт пояс счётчика модельного мира. DDS отдаёт event_time
-- в этой временной линзе.
CREATE VIEW IF NOT EXISTS dds.event_v ON CLUSTER clickstream_cluster
AS
SELECT
    WatchID AS event_id,
    ClientID AS client_id,
    CounterID AS counter_id,
    EventDate AS event_date,
    toTimeZone(UTCEventTime, 'Europe/Samara') AS event_time,
    ClientTimeZone AS client_timezone,
    EventType AS event_type,
    URL AS url,
    Referer AS referer,
    Title AS title,
    UTMSource AS utm_source,
    UTMMedium AS utm_medium,
    UTMCampaign AS utm_campaign,
    UTMContent AS utm_content,
    UTMTerm AS utm_term,
    LastTrafficSource AS last_traffic_source,
    HasGCLID AS has_gclid,
    YCLID AS yclid,
    Browser AS browser,
    BrowserMajorVersion AS browser_major_version,
    BrowserLanguage AS browser_language,
    OperatingSystem AS operating_system,
    OperatingSystemRoot AS operating_system_root,
    transform(
        DeviceCategory,
        [1, 2, 3, 4],
        ['desktop', 'mobile', 'tablet', 'tv'],
        toString(DeviceCategory)
    ) AS device_category,
    MobilePhoneModel AS mobile_phone_model,
    ScreenWidth AS screen_width,
    ScreenHeight AS screen_height,
    IPAddress AS ip_address,
    RegionCountry AS region_country,
    RegionCity AS region_city,
    RegionCountryID AS region_country_id,
    RegionCityID AS region_city_id,
    GoalsReached AS goals_reached,
    ParsedParamsKey1 AS parsed_params_key1,
    purchaseID AS purchase_id,
    purchaseRevenue AS purchase_revenue,
    purchaseCurrency AS purchase_currency,
    purchaseCoupon AS purchase_coupon,
    productID AS product_id,
    productName AS product_name,
    productCategory AS product_category,
    productPrice AS product_price,
    productQuantity AS product_quantity,
    productEventType AS product_event_type
FROM ods.event_v;
