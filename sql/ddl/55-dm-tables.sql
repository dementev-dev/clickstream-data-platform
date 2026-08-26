-- DM: физические дневные витрины и донор партиций выручки.

-- Выручка хранится в зерне «день заказа × категория товара». День служит
-- единицей пересборки, поэтому совпадает с ключом партиции.
CREATE TABLE IF NOT EXISTS dm.revenue_daily_rep ON CLUSTER clickstream_cluster
(
    report_date Date,
    product_category LowCardinality(String),
    orders UInt64,
    units UInt64,
    revenue Decimal(18, 2),
    aov Decimal(18, 2),
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY report_date
ORDER BY (report_date, product_category);

-- Категория — единственное измерение кроме дня. Шесть категорий стартового
-- мира близки по объему, поэтому простой ключ раскладывает строки достаточно
-- ровно и не добавляет в учебный пример составной хеш без читателя.
CREATE TABLE IF NOT EXISTS dm.revenue_daily_dist ON CLUSTER clickstream_cluster
AS dm.revenue_daily_rep
ENGINE = Distributed(
    'clickstream_cluster',
    'dm',
    'revenue_daily_rep',
    cityHash64(product_category)
);

-- Донор объявлен через AS и шардируется тем же ключом, что цель. REPLACE
-- PARTITION идет по локальным таблицам, поэтому структура и раскладка донора
-- обязаны совпадать с целью (docs/architecture/storage.md, «Раскладка по
-- шардам»).
CREATE TABLE IF NOT EXISTS dm.revenue_daily_stage_rep ON CLUSTER clickstream_cluster
AS dm.revenue_daily_rep
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY report_date
ORDER BY (report_date, product_category);

CREATE TABLE IF NOT EXISTS dm.revenue_daily_stage_dist ON CLUSTER clickstream_cluster
AS dm.revenue_daily_stage_rep
ENGINE = Distributed(
    'clickstream_cluster',
    'dm',
    'revenue_daily_stage_rep',
    cityHash64(product_category)
);

-- Трафик пересчитывается целиком: новая связь куки с пользователем меняет
-- прошлые дни. Версия запуска позволяет точному представлению скрыть строки
-- прежних пересчетов без TRUNCATE и без партиций.
CREATE TABLE IF NOT EXISTS dm.daily_traffic_rep ON CLUSTER clickstream_cluster
(
    report_date Date,
    visitors UInt64,
    known_users UInt64,
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}',
    '{replica}',
    _load_ts
)
ORDER BY report_date;

-- Все версии одного дня должны попадать на один шард, иначе FINAL не сможет
-- выбрать последнюю через границу шардов.
CREATE TABLE IF NOT EXISTS dm.daily_traffic_dist ON CLUSTER clickstream_cluster
AS dm.daily_traffic_rep
ENGINE = Distributed(
    'clickstream_cluster',
    'dm',
    'daily_traffic_rep',
    cityHash64(report_date)
);
