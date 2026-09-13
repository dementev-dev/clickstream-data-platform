-- DM: расчет выручки за дни от first_day до last_day включительно.
-- DROP PARTITION ALL очищает промежуточную revenue_daily_stage_rep.
-- Затем INSERT записывает по одной строке итогов на день и категорию товара.
-- Основная revenue_daily_rep остается прежней до revenue_daily_replace.sql.
-- В расчет входят только оплаченные заказы. ARRAY JOIN превращает артикул,
-- количество и цену с одним индексом в массивах в одну строку позиции заказа.
-- Причина выбора DROP вместо TRUNCATE — в sql/dds/order_rebuild.sql.
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
    -- Делим с шестью знаками после запятой, затем округляем до копеек.
    -- Так дробная часть сохраняется до округления среднего чека.
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
-- Источник распределен по заказу, цель — по категории. Значение 0 оставляет
-- итоговую агрегацию и распределение по ключу цели на ноде, принявшей запрос.
-- distributed_foreground_insert = 1 ждет доставки строк перед заменой.
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
