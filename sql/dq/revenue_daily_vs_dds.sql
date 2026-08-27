-- DQ: дневная выручка против прямого агрегата заказов DDS.
--
-- День берётся из объединения сторон. Поэтому исчезнувший целиком день или
-- категория остаются видимы. Денежный расчет повторяет публичный договор
-- витрины и сохраняет Decimal до округления AOV.
WITH
    reference AS
    (
        SELECT
            report_date,
            product_category,
            orders,
            units,
            CAST(revenue, 'Decimal(18, 2)') AS revenue,
            CAST(
                round(divideDecimal(revenue, toDecimal128(orders, 0), 6), 2),
                'Decimal(18, 2)'
            ) AS aov
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
            GROUP BY
                report_date,
                product_category
        )
    ),
    actual AS
    (
        SELECT
            report_date,
            product_category,
            count() AS physical_rows,
            any(orders) AS orders,
            any(units) AS units,
            any(revenue) AS revenue,
            any(aov) AS aov
        FROM dm.revenue_daily_v
        GROUP BY
            report_date,
            product_category
    ),
    comparison AS
    (
        SELECT
            coalesce(reference.report_date, actual.report_date) AS data_date,
            coalesce(reference.product_category, actual.product_category)
                AS business_key,
            isNotNull(reference.product_category) AS reference_present,
            isNotNull(actual.product_category) AS actual_present,
            toString(tuple(
                reference.orders,
                reference.units,
                reference.revenue,
                reference.aov
            )) AS reference_value,
            concat(
                toString(tuple(
                    actual.orders,
                    actual.units,
                    actual.revenue,
                    actual.aov
                )),
                ', rows=',
                toString(actual.physical_rows)
            ) AS actual_value,
            isNull(reference.product_category)
                OR isNull(actual.product_category)
                OR actual.physical_rows != 1
                OR NOT isNotDistinctFrom(reference.orders, actual.orders)
                OR NOT isNotDistinctFrom(reference.units, actual.units)
                OR NOT isNotDistinctFrom(reference.revenue, actual.revenue)
                OR NOT isNotDistinctFrom(reference.aov, actual.aov) AS failed
        FROM reference
        GLOBAL FULL OUTER JOIN actual USING (report_date, product_category)
    )
SELECT
    data_date,
    countIf(reference_present) AS reference_rows,
    countIf(actual_present) AS actual_rows,
    countIf(failed) AS failed_rows,
    groupArrayIf(20)(
        concat(
            toString(data_date),
            ' key=',
            business_key,
            ': эталон=',
            if(reference_present, reference_value, 'нет ключа'),
            ', объект=',
            if(actual_present, actual_value, 'нет ключа')
        ),
        failed
    ) AS diagnostics
FROM comparison
GROUP BY data_date
ORDER BY data_date
SETTINGS join_use_nulls = 1
