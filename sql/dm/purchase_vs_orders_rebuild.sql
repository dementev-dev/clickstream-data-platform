-- DM: сверка покупок трекера с текущими заказами бэкенда.
--
-- Обе стороны FULL OUTER JOIN — подзапросы. С голой левой Distributed
-- ClickHouse вернул бы непарный заказ по разу с каждого шарда; форма измерена
-- в docs/research/2026-08-22-distributed-join.md.
-- Выбор DROP PARTITION ALL вместо TRUNCATE разобран в соседнем
-- sql/dds/order_rebuild.sql.

ALTER TABLE dm.purchase_vs_orders_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;

INSERT INTO dm.purchase_vs_orders_stage_dist
(
    order_day,
    order_id,
    declared_revenue,
    items_total,
    status,
    mismatch_class,
    _load_id,
    _load_ts
)
SELECT
    -- У парного заказа день берется у бэкенда; трекер дает его только
    -- непарной покупке.
    coalesce(order_date, purchase_day),
    coalesce(purchase_order_id, backend_order_id),
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
    ),
    {load_id:String},
    now64(3, 'UTC')
FROM
(
    -- Контракт purchase несет один номер и одну объявленную сумму в массивах.
    -- Бизнес-дубль повторяет оба значения, поэтому count хранится отдельно,
    -- а сумма берется одна — складывать дубли значило бы выдумать дельту.
    SELECT
        purchase_id[1] AS purchase_order_id,
        any(event_date) AS purchase_day,
        any(purchase_revenue[1]) AS declared_revenue,
        count() AS purchase_count
    FROM dds.event_v
    WHERE event_type = 'purchase'
        AND event_date BETWEEN {first_day:Date} AND {last_day:Date}
    GROUP BY purchase_order_id
) AS purchases
GLOBAL FULL OUTER JOIN
(
    SELECT
        order_id AS backend_order_id,
        order_date,
        items_total,
        status
    FROM dds.order_v
    WHERE order_date BETWEEN {first_day:Date} AND {last_day:Date}
) AS orders ON purchase_order_id = backend_order_id
-- NULL обозначает отсутствующую сторону. При join_use_nulls = 0 ClickHouse
-- заполнил бы ее нулем или пустой строкой и скрыл отсутствие источника.
-- Вставка идет через ключ цели: источники разложены по другим ключам.
SETTINGS join_use_nulls = 1,
    distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
