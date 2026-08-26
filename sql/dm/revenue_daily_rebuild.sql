-- DM: сборка выручки в донор перед заменой дневных партиций.
-- DROP PARTITION ALL очищает локальные таблицы на каждом узле; причина не
-- применять TRUNCATE разобрана в соседнем sql/dds/order_rebuild.sql.

ALTER TABLE dm.revenue_daily_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;

INSERT INTO dm.revenue_daily_stage_dist
(
    report_date,
    product_category,
    orders,
    units,
    revenue,
    aov,
    _load_id,
    _load_ts
)
SELECT
    report_date,
    product_category,
    orders,
    units,
    CAST(revenue, 'Decimal(18, 2)'),
    -- Обычное деление Decimal усекает доли копейки вместо округления;
    -- запас масштаба сохраняет их до денежного результата.
    CAST(
        round(divideDecimal(revenue, toDecimal128(orders, 0), 6), 2),
        'Decimal(18, 2)'
    ),
    {load_id:String},
    now64(3, 'UTC')
FROM
(
    SELECT
        order_date AS report_date,
        dictGet('dic.products', 'category', item_sku) AS product_category,
        uniqExact(order_id) AS orders,
        sum(item_quantity) AS units,
        sum(item_price * item_quantity) AS revenue
    FROM dds.order_v
    ARRAY JOIN
        item_sku,
        item_quantity,
        item_price
    WHERE status = 'paid'
        AND order_date BETWEEN {first_day:Date} AND {last_day:Date}
    GROUP BY
        report_date,
        product_category
)
-- Источник лежит по заказу, цель — по категории. Ноль заставляет инициатор
-- слить частичные агрегаты и разложить готовые строки по ключу цели.
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
