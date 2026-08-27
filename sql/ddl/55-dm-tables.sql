-- DM: физические дневные витрины и доноры их партиций.

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

-- Сверка хранит одну строку на заказ. День — единица пересборки: пока рядом
-- живут два источника, строка может перейти из awaiting_order в обычный класс.
CREATE TABLE IF NOT EXISTS dm.purchase_vs_orders_rep ON CLUSTER clickstream_cluster
(
    order_day Date,
    order_id String,
    declared_revenue Nullable(Decimal(18, 2)),
    items_total Nullable(Decimal(18, 2)),
    status LowCardinality(Nullable(String)),
    mismatch_class LowCardinality(String),
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY order_day
ORDER BY (order_day, order_id);

-- Ключ совпадает с DDS, поэтому строка заказа на всех слоях попадает на один
-- шард.
CREATE TABLE IF NOT EXISTS dm.purchase_vs_orders_dist ON CLUSTER clickstream_cluster
AS dm.purchase_vs_orders_rep
ENGINE = Distributed(
    'clickstream_cluster',
    'dm',
    'purchase_vs_orders_rep',
    cityHash64(order_id)
);

-- Донор повторяет структуру и раскладку цели по правилу, разобранному у
-- revenue_daily_stage_rep и в docs/architecture/storage.md.
CREATE TABLE IF NOT EXISTS dm.purchase_vs_orders_stage_rep ON CLUSTER clickstream_cluster
AS dm.purchase_vs_orders_rep
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY order_day
ORDER BY (order_day, order_id);

CREATE TABLE IF NOT EXISTS dm.purchase_vs_orders_stage_dist ON CLUSTER clickstream_cluster
AS dm.purchase_vs_orders_stage_rep
ENGINE = Distributed(
    'clickstream_cluster',
    'dm',
    'purchase_vs_orders_stage_rep',
    cityHash64(order_id)
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

-- Сводка качества хранит текущее состояние атомарной проверки одного дня.
-- История запусков остаётся в Airflow, поэтому версия строки не нужна:
-- следующий успешный прогон заменяет дневную партицию целиком.
CREATE TABLE IF NOT EXISTS dm.dq_summary_rep ON CLUSTER clickstream_cluster
(
    data_date Date,
    check_name LowCardinality(String),
    reference_rows UInt64,
    actual_rows UInt64,
    failed_rows UInt64,
    status LowCardinality(String),
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY data_date
ORDER BY (data_date, check_name);

-- Все строки одной проверки лежат на одном шарде. Таблица мала, а ключ
-- check_name делает раскладку донора и цели одинаковой без нового измерения.
CREATE TABLE IF NOT EXISTS dm.dq_summary_dist ON CLUSTER clickstream_cluster
AS dm.dq_summary_rep
ENGINE = Distributed(
    'clickstream_cluster',
    'dm',
    'dq_summary_rep',
    cityHash64(check_name)
);

-- Источник и цель REPLACE PARTITION обязаны совпадать по структуре, ключам и
-- политике хранения. Требование повторно сверено по документации ClickHouse
-- через Context7 27 августа 2026 года; AS не даёт формам разойтись.
CREATE TABLE IF NOT EXISTS dm.dq_summary_stage_rep ON CLUSTER clickstream_cluster
AS dm.dq_summary_rep
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY data_date
ORDER BY (data_date, check_name);

CREATE TABLE IF NOT EXISTS dm.dq_summary_stage_dist ON CLUSTER clickstream_cluster
AS dm.dq_summary_stage_rep
ENGINE = Distributed(
    'clickstream_cluster',
    'dm',
    'dq_summary_stage_rep',
    cityHash64(check_name)
);
