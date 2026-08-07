-- ODS: типизированное событие и таблица ошибок разбора.
--
-- Состав, имена и типы колонок списаны с «описания выгрузки»
-- (docs/formats/clickstream-event.md) — так же, как в бою хранилище пишут по
-- документации источника. Модуль контракта генератора здесь не читается:
-- граница «трекер | хранилище» проходит по документу.
-- Механика приёма и три класса брака — ADR 0005, конвенции имён, служебных
-- колонок и сроков — docs/architecture/storage.md.

-- Локальная таблица события.
--
-- Движок ReplacingMergeTree, колонка версии — _load_ts. Работа у версии ровно
-- одна: схлопнуть повтор доставки. Метка не ставится здесь заново, а
-- переносится из stg.hits_raw как есть — колонка отвечает на вопрос «когда
-- строка приехала в хранилище», а не «когда её разобрали». У повтора
-- содержимое то же самое, отличается только метка, поэтому какая из двух
-- строк переживёт мерж, безразлично.
--
-- Колонка версии не входит ни в ключ партиции, ни в ключ сортировки, и это
-- не случайность: попади она туда — версии одной строки лягут в разные куски
-- или в разные места ключа и не встретятся при мерже, то есть дедупликация
-- перестанет работать молча.
--
-- PARTITION BY EventDate — по дню события, а не по месяцу, как у Метрики.
-- Отступление осознанное: дневная партиция здесь единица переобработки, и
-- переиграть день X значит заменить одну партицию. Месячная партиция тянула
-- бы за собой тридцать чужих дней.
--
-- ORDER BY вырожден, и это учебный факт, а не недосмотр: CounterID на стенде
-- константа (сайт один), EventDate константа внутри своей партиции — обе
-- головные колонки ключа не различают ни одной строки. Реальная сортировка
-- идёт по посетителю и событию: intHash32(ClientID), WatchID. Ключ написан в
-- боевой форме «по сайту за период по посетителю» — на стенде она
-- вырождается, в бою нет. Хвост WatchID работает ещё и на дедупликацию: без
-- него ReplacingMergeTree схлопнул бы в одну строку все события посетителя за
-- день.
--
-- SAMPLE BY intHash32(ClientID) — по тому же выражению, что стоит в ключе
-- сортировки (иначе семплирование не разрешено). Семплирование берёт целиком
-- посетителей, а не события вразнобой, поэтому SAMPLE 0.1 не портит
-- uniq-метрики.
--
-- Sign всегда равен 1: это колонка формата, взятая без механики. В бою
-- исправление записи шлют парой −1/+1 и считают через sum(Sign) поверх
-- CollapsingMergeTree; наш генератор исправлений не шлёт, поэтому колонка
-- есть, а механики за ней нет.
CREATE TABLE IF NOT EXISTS ods.event_rep ON CLUSTER clickstream_cluster
(
    WatchID UInt64,
    VisitID UInt64,
    ClientID UInt64,
    CounterID UInt32,
    EventDate Date,
    UTCEventTime DateTime,
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
    purchaseRevenue Array(Float64),
    purchaseCurrency Array(String),
    purchaseCoupon Array(String),
    productID Array(String),
    productName Array(String),
    productCategory Array(String),
    productPrice Array(Int64),
    productQuantity Array(UInt64),
    productEventType Array(String),
    ecommerce String,
    _load_ts DateTime64(3)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', _load_ts)
PARTITION BY EventDate
ORDER BY (CounterID, EventDate, intHash32(ClientID), WatchID)
SAMPLE BY intHash32(ClientID);

-- Лицо слоя: пишем и читаем через него. Ключ шардирования —
-- cityHash64(ClientID), а не сырой ClientID: структурированный числовой
-- идентификатор перекашивает остаток по модулю числа шардов, хеш — нет.
-- События одной куки при этом остаются на одном шарде, и сессионизация со
-- склейкой идентичностей ниже по течению живут локально.
--
-- У сырья ключ другой (хеш строки), поэтому сырая строка и разобранное из неё
-- событие почти всегда лежат на разных шардах. Пошардовые счётчики слоёв
-- сходиться не должны — сверять слои можно только через _dist.
CREATE TABLE IF NOT EXISTS ods.event_dist ON CLUSTER clickstream_cluster
AS ods.event_rep
ENGINE = Distributed('clickstream_cluster', 'ods', 'event_rep', cityHash64(ClientID));

-- Локальная таблица ошибок разбора.
--
-- Сырой текст сообщения, метаданные доставки и класс брака. Разобранных полей
-- у брака нет по определению: строка сюда попала как раз потому, что не
-- разобралась.
--
-- Движок — обычный ReplicatedMergeTree, без замены версий: у брака нет ключа
-- сущности, схлопывать его не по чему.
--
-- Ключи собственные. Шардирование — cityHash64(raw): ClientID у неразобранной
-- строки взять неоткуда. ORDER BY (error_class, kafka_partition,
-- kafka_offset): такую таблицу смотрят от класса брака, а внутри класса — по
-- координатам доставки. Нарезка по дню загрузки, как у сырья: модельного дня
-- у брака тоже нет.
--
-- Срок жизни — месяц, вдесятеро дольше сырья. Истеки брак вместе с сырьём —
-- к моменту разбирательства не осталось бы ни того, ни другого.
CREATE TABLE IF NOT EXISTS ods.event_errors_rep ON CLUSTER clickstream_cluster
(
    raw String,
    error_class LowCardinality(String),
    kafka_topic LowCardinality(String),
    kafka_partition UInt64,
    kafka_offset UInt64,
    kafka_timestamp Nullable(DateTime64(3)),
    consumer_host LowCardinality(String),
    _load_ts DateTime64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toDate(_load_ts)
ORDER BY (error_class, kafka_partition, kafka_offset)
TTL toDateTime(_load_ts) + INTERVAL 1 MONTH
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS ods.event_errors_dist ON CLUSTER clickstream_cluster
AS ods.event_errors_rep
ENGINE = Distributed('clickstream_cluster', 'ods', 'event_errors_rep', cityHash64(raw));
