-- DM: публичные договоры стендовых витрин.

-- День и категория — зерно выручки. Служебные колонки показывают запуск,
-- который последним заменил дневную партицию.
CREATE VIEW IF NOT EXISTS dm.revenue_daily_v ON CLUSTER clickstream_cluster
AS
SELECT
    report_date,
    product_category,
    orders,
    units,
    revenue,
    aov,
    _load_id,
    _load_ts
FROM dm.revenue_daily_dist;

-- Полный пересчет пишет новую версию каждого дня. FINAL оставляет текущие
-- счетчики и прячет повторы запусков от потребителя.
CREATE VIEW IF NOT EXISTS dm.daily_traffic_v ON CLUSTER clickstream_cluster
AS
SELECT
    report_date,
    visitors,
    known_users,
    _load_id,
    _load_ts
FROM dm.daily_traffic_dist FINAL;
