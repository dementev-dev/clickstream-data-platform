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

-- DDS: публичный договор заказа на языке бизнес-модели.
--
-- В отличие от события, у заказа под представлением лежит физическая модель:
-- разбор позиций и пересчёт пояса стоят денег, и платить их при каждом чтении
-- значило бы собирать модель заново на лету (docs/architecture/dds/order.md,
-- «Отклонённые варианты»). Представление здесь ничего не вычисляет — оно
-- только называет колонки, которые слой обещает читателю.
--
-- FINAL нет и не будет: строка на заказ одна по построению, повтор гасит
-- замена партиции, а не колонка версии.
--
-- Служебные колонки перечислены наравне с деловыми — это координаты
-- собственной загрузки слоя, и расследование происшествия начинается с них:
-- по _load_id видно, какой запуск собрал день.
CREATE VIEW IF NOT EXISTS dds.order_v ON CLUSTER clickstream_cluster
AS
SELECT
    order_id,
    user_id,
    status,
    order_date,
    created_at,
    updated_at,
    items_total,
    discount,
    delivery,
    total,
    item_sku,
    item_quantity,
    item_price,
    _load_id,
    _load_ts
FROM dds.order_dist;

-- DDS: публичный договор сессии на языке бизнес-модели.
--
-- Под представлением — физическая модель, как у заказа: оконная нарезка
-- визитов на каждое чтение означала бы считать сущность заново в каждом
-- отчёте (docs/architecture/dds/session.md, «Отклонённые варианты»).
--
-- Эталонного VisitID в договоре нет намеренно: наша нарезка обязана уметь
-- разойтись с эталоном, на этом стоит сверка. За эталоном она ходит в
-- ods.event_v.
CREATE VIEW IF NOT EXISTS dds.session_v ON CLUSTER clickstream_cluster
AS
SELECT
    session_id,
    client_id,
    session_date,
    started_at,
    finished_at,
    duration_seconds,
    events_total,
    pageviews,
    cart_adds,
    purchases,
    entry_url,
    exit_url,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    device_category,
    region_city,
    _load_id,
    _load_ts
FROM dds.session_dist;

-- DDS: точная известная связь куки с пользователем магазина.
--
-- Полный пересчёт оставляет физические версии одной пары до слияния частей.
-- FINAL прячет это устройство от читателя; явный список колонок не даёт
-- служебным изменениям таблицы случайно расширить договор.
CREATE VIEW IF NOT EXISTS dds.identity_map_v ON CLUSTER clickstream_cluster
AS
SELECT
    client_id,
    user_id,
    _load_id,
    _load_ts
FROM dds.identity_map_dist FINAL;
