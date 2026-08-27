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

-- Непарные стороны остаются NULL. При join_use_nulls = 0 ClickHouse заполнил
-- бы их нулями и пустыми строками, скрыв отсутствие источника.
-- Служебные колонки называют запуск, который последним заменил день.
CREATE VIEW IF NOT EXISTS dm.purchase_vs_orders_v ON CLUSTER clickstream_cluster
AS
SELECT
    order_day,
    order_id,
    declared_revenue,
    items_total,
    status,
    mismatch_class,
    _load_id,
    _load_ts
FROM dm.purchase_vs_orders_dist;

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

-- Сводка отвечает, какие атомарные договоры дня выполняются сейчас.
-- Явный список не пропускает в публичный договор будущие физические колонки.
CREATE VIEW IF NOT EXISTS dm.dq_summary_v ON CLUSTER clickstream_cluster
AS
SELECT
    data_date,
    check_name,
    reference_rows,
    actual_rows,
    failed_rows,
    status,
    _load_id,
    _load_ts
FROM dm.dq_summary_dist;
