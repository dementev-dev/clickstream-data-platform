-- Локальная таблица события.
-- Поля следуют docs/formats/clickstream-event.md; purchaseRevenue хранится
-- в Decimal для точного счета денег. Правила приема — ADR 0005.
-- ReplacingMergeTree оставляет последнюю доставку по _load_ts среди строк
-- с одинаковым полным ORDER BY. Метка переносится из STG без пересчета.
-- Она не входит в ключ сортировки или партиции: иначе повторные доставки
-- не считались бы версиями одной строки. Для чтения служит ods.event_v с FINAL.
--
-- Партиция EventDate хранит модельный день события. CounterID на стенде
-- постоянен, EventDate постоянна внутри партиции; дальнейшая сортировка —
-- по intHash32(ClientID) и WatchID. WatchID отличает события посетителя:
-- без него они заменяли бы друг друга.
--
-- SAMPLE BY использует то же выражение, что ключ сортировки, и отбирает
-- посетителей целиком. Sign всегда равен 1: поле сохранено по формату
-- источника, но исправлений парой −1/+1 генератор не отправляет.

CREATE TABLE IF NOT EXISTS ods.event_rep ON CLUSTER clickstream_cluster
(
    WatchID UInt64,
    VisitID UInt64,
    ClientID UInt64,
    CounterID UInt32,
    EventDate Date,
    UTCEventTime DateTime('UTC'),
    ClientTimeZone Int16,
    EventType LowCardinality(String),
    Sign Int8,
    URL String,
    Referer String,
    Title String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    LastTrafficSource String,
    HasGCLID UInt8,
    YCLID UInt64,
    Browser String,
    BrowserMajorVersion UInt16,
    BrowserLanguage String,
    OperatingSystem String,
    OperatingSystemRoot String,
    DeviceCategory UInt8,
    MobilePhoneModel String,
    ScreenWidth UInt16,
    ScreenHeight UInt16,
    IPAddress String,
    RegionCountry String,
    RegionCity String,
    RegionCountryID UInt32,
    RegionCityID UInt32,
    GoalsReached Array(UInt32),
    ParsedParamsKey1 Array(String),
    purchaseID Array(String),
    purchaseRevenue Array(Decimal(18, 2)),
    purchaseCurrency Array(String),
    purchaseCoupon Array(String),
    productID Array(String),
    productName Array(String),
    productCategory Array(String),
    productPrice Array(Int64),
    productQuantity Array(UInt64),
    productEventType Array(String),
    ecommerce String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', _load_ts)
PARTITION BY EventDate
ORDER BY (CounterID, EventDate, intHash32(ClientID), WatchID)
SAMPLE BY intHash32(ClientID);

-- Запись и диагностика через Distributed; актуальные события — в ods.event_v.
-- cityHash64(ClientID) распределяет посетителей равномернее, чем остаток
-- самого структурированного идентификатора. События посетителя лежат вместе.
-- STG распределен по хешу raw, поэтому сообщение и разобранное событие могут
-- лежать на разных шардах. Общие счетчики сравнивают через _dist.

CREATE TABLE IF NOT EXISTS ods.event_dist ON CLUSTER clickstream_cluster
AS ods.event_rep
ENGINE = Distributed('clickstream_cluster', 'ods', 'event_rep', cityHash64(ClientID));

-- Ошибки разбора: исходный текст, класс и сведения о доставке.
-- У сообщения может не быть разобранного ClientID, поэтому шардирование —
-- по cityHash64(raw), партиция — по реальному дню загрузки.
-- ORDER BY группирует ошибки по классу и координатам Kafka.
--
-- Замены версий нет: надежного ключа события у брака может не быть.
-- TTL сохраняет ошибки месяц, дольше трехсуточного срока STG.

CREATE TABLE IF NOT EXISTS ods.event_errors_rep ON CLUSTER clickstream_cluster
(
    raw String,
    error_class LowCardinality(String),
    kafka_topic LowCardinality(String),
    kafka_partition UInt64,
    kafka_offset UInt64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),
    consumer_host LowCardinality(String),
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toDate(_load_ts)
ORDER BY (error_class, kafka_partition, kafka_offset)
TTL toDateTime(_load_ts) + INTERVAL 1 MONTH
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS ods.event_errors_dist ON CLUSTER clickstream_cluster
AS ods.event_errors_rep
ENGINE = Distributed('clickstream_cluster', 'ods', 'event_errors_rep', cityHash64(raw));

-- Локальная таблица версий заказа.
--
-- Слепок привозит состояние заказов окна изменяемости, а не поток изменений,
-- и одна и та же сущность приезжает в нём много дней подряд. Поэтому строка
-- здесь — версия заказа, а не запись слепка: ключ сущности order_id, колонка
-- версии snapshot_date — день, состояние которого снял источник. У полного
-- слепка версия и есть слепок: истинно то, что источник сказал последним
-- (ADR 0014).
--
-- Четыре координаты отвечают на разные вопросы, и путать их нельзя:
-- snapshot_date — в слепке какого модельного дня источник показал строку;
-- updated_at — когда строку последний раз меняли в источнике; _load_id —
-- какой запуск Airflow её принял; _load_ts — когда она приехала в хранилище.
-- Ключом сущности не становится ни одна: заказ один, а дней наблюдения и
-- версий у него много. Схлопывание оставляет строку позднейшего слепка,
-- поэтому snapshot_date выжившей — последний день, когда источник показывал
-- заказ, и по нему DDS читает объём пересборки
-- (docs/architecture/orders/ingestion.md).
--
-- PARTITION BY toDate(created_at) — по дню создания строки в источнике. Он
-- неизменен у всех версий заказа, поэтому версии лежат в одной партиции и
-- встречаются при мерже. Днём покупки этот день не является: бизнес-время
-- живёт в событии purchase (docs/research/2026-08-16-order-snapshot-wire-format.md).
--
-- Замена партиции сюда не годится и заменена версиями: у прямого чтения Kafka
-- нет признака конца слепка, а дата наблюдения не ключ публикации (ADR 0010).
--
-- items остаётся сырым фрагментом JSON: приём проверяет только, что это
-- массив. Что внутри позиций — забота DDS, а не границы провода.
CREATE TABLE IF NOT EXISTS ods.order_rep ON CLUSTER clickstream_cluster
(
    order_id String,
    user_id UInt64,
    status LowCardinality(String),
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    items_total Decimal(18, 2),
    discount Decimal(18, 2),
    delivery Decimal(18, 2),
    total Decimal(18, 2),
    items String,
    snapshot_date Date,
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', snapshot_date)
PARTITION BY toDate(created_at)
ORDER BY order_id;

-- Лицо слоя: пишем и читаем через него. Ключ шардирования — cityHash64(order_id),
-- и выбор здесь не про перекос, а про корректность: только так все версии
-- одного заказа попадают на один шард, и FINAL через распределённую таблицу
-- выбирает одного победителя, а не по победителю на шард.
CREATE TABLE IF NOT EXISTS ods.order_dist ON CLUSTER clickstream_cluster
AS ods.order_rep
ENGINE = Distributed('clickstream_cluster', 'ods', 'order_rep', cityHash64(order_id));

-- Локальная таблица брака слепка.
--
-- Устроена как ods.event_errors_rep и по тем же доводам (см. выше): сырой
-- текст, метаданные доставки, класс брака, нарезка по дню загрузки, месяц
-- жизни, снятие кусками целиком. Своя колонка одна — _load_id: по нему видно,
-- какой запуск привёз брак, и повтор задачи узнаётся по совпадению _load_id
-- с координатами доставки.
--
-- Классы у заказов свои и их три: not_an_object, keyset_mismatch,
-- field_invalid. Имя провалившегося поля в класс не входит — сырой текст лежит
-- рядом, и единичный случай разбирается по нему, без постоянной детализации
-- предиката (docs/architecture/orders/ingestion.md).
CREATE TABLE IF NOT EXISTS ods.order_errors_rep ON CLUSTER clickstream_cluster
(
    raw String,
    error_class LowCardinality(String),
    kafka_topic LowCardinality(String),
    kafka_partition UInt64,
    kafka_offset UInt64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),
    consumer_host LowCardinality(String),
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toDate(_load_ts)
ORDER BY (error_class, kafka_partition, kafka_offset)
TTL toDateTime(_load_ts) + INTERVAL 1 MONTH
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS ods.order_errors_dist ON CLUSTER clickstream_cluster
AS ods.order_errors_rep
ENGINE = Distributed('clickstream_cluster', 'ods', 'order_errors_rep', cityHash64(raw));
