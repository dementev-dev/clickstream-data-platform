-- DQ: строка сверки покупок и заказов против независимой сборки из DDS.
--
-- Проверяются все деловые поля. Счёт физических строк отдельно ловит дубль
-- ключа, даже если обе копии несут одинаковые значения.
WITH
    purchases AS
    (
        SELECT
            purchase_id[1] AS purchase_order_id,
            any(event_date) AS purchase_day,
            any(purchase_revenue[1]) AS declared_revenue,
            count() AS purchase_count
        FROM dds.event_v
        WHERE event_type = 'purchase'
        GROUP BY purchase_order_id
    ),
    orders AS
    (
        SELECT
            order_id AS backend_order_id,
            order_date,
            items_total,
            status
        FROM dds.order_v
    ),
    reference AS
    (
        SELECT
            coalesce(order_date, purchase_day) AS order_day,
            coalesce(purchase_order_id, backend_order_id) AS order_id,
            declared_revenue,
            items_total,
            status,
            multiIf(
                backend_order_id IS NULL, 'awaiting_order',
                status = 'cancelled', 'cancelled',
                purchase_order_id IS NULL, 'lost_event',
                purchase_count > 1, 'duplicate_event',
                declared_revenue != items_total, 'amount_delta',
                'match'
            ) AS mismatch_class
        FROM purchases
        GLOBAL FULL OUTER JOIN orders
            ON purchase_order_id = backend_order_id
    ),
    actual AS
    (
        SELECT
            order_day,
            order_id,
            count() AS physical_rows,
            any(declared_revenue) AS declared_revenue,
            any(items_total) AS items_total,
            any(status) AS status,
            any(mismatch_class) AS mismatch_class
        FROM dm.purchase_vs_orders_v
        GROUP BY
            order_day,
            order_id
    )
SELECT
    coalesce(reference.order_day, actual.order_day) AS data_date,
    coalesce(reference.order_id, actual.order_id) AS business_key,
    isNotNull(reference.order_id) AS reference_present,
    isNotNull(actual.order_id) AS actual_present,
    toString(tuple(
        reference.declared_revenue,
        reference.items_total,
        reference.status,
        reference.mismatch_class
    )) AS reference_value,
    concat(
        toString(tuple(
            actual.declared_revenue,
            actual.items_total,
            actual.status,
            actual.mismatch_class
        )),
        ', rows=',
        toString(actual.physical_rows)
    ) AS actual_value,
    isNull(reference.order_id)
        OR isNull(actual.order_id)
        OR actual.physical_rows != 1
        OR NOT isNotDistinctFrom(reference.declared_revenue, actual.declared_revenue)
        OR NOT isNotDistinctFrom(reference.items_total, actual.items_total)
        OR NOT isNotDistinctFrom(reference.status, actual.status)
        OR NOT isNotDistinctFrom(reference.mismatch_class, actual.mismatch_class)
        AS failed
FROM reference
GLOBAL FULL OUTER JOIN actual USING (order_day, order_id)
ORDER BY
    data_date,
    business_key
SETTINGS join_use_nulls = 1
