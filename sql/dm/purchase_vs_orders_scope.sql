-- DM: объем пересборки сверки покупок с заказами.
--
-- У источников разные правые границы. Заказы называют изменяемое окно ярлыком
-- последнего принятого слепка. События заканчиваются последним прожитым днем,
-- который выводится из позиции мира. Одна граница другую не подменяет.
--
-- Левая граница возвращает не только пропуски, но и самый ранний
-- awaiting_order. Такой день уже есть в витрине, поэтому обычный поиск дыры
-- его не увидит. Иначе пакетный разгон длиннее K навсегда оставил бы прежний
-- хвост предварительным.
--
-- Файл заканчивается без точки с запятой: почему — sql/dds/order_scope.sql.
WITH
    (SELECT min(event_date) FROM dds.event_v) AS first_event_day,
    first_event_day + ({position:UInt16} - 1) AS last_lived_day,
    (SELECT max(snapshot_date) FROM ods.order_dist) AS last_snapshot,
    (
        SELECT min(toDate(toTimeZone(created_at, 'Europe/Samara')))
        FROM ods.order_dist
        WHERE snapshot_date = last_snapshot
    ) AS window_start,
    (
        SELECT max(toDate(toTimeZone(created_at, 'Europe/Samara')))
        FROM ods.order_dist
        WHERE snapshot_date = last_snapshot
    ) AS window_end,
    (
        SELECT maxOrNull(event_date)
        FROM dds.event_v
        WHERE event_type = 'purchase' AND event_date <= last_lived_day
    ) AS last_purchase_day,
    (
        SELECT minOrNull(day)
        FROM (
            SELECT order_date AS day
            FROM dds.order_v
            UNION DISTINCT
            SELECT event_date AS day
            FROM dds.event_v
            WHERE event_type = 'purchase' AND event_date <= last_lived_day
        )
        WHERE day GLOBAL NOT IN (
            SELECT order_day FROM dm.purchase_vs_orders_dist
        )
    ) AS first_missing_day,
    (
        SELECT minOrNull(order_day)
        FROM dm.purchase_vs_orders_dist
        WHERE mismatch_class = 'awaiting_order'
    ) AS first_provisional_day
SELECT
    least(
        window_start,
        coalesce(first_missing_day, window_start),
        coalesce(first_provisional_day, window_start)
    ) AS first_day,
    greatest(window_end, last_purchase_day) AS last_day
-- Без принятого слепка неизвестно окно заказов. Без готового DDS или прожитой
-- покупки донор был бы пуст, поэтому подменять партиции еще нельзя.
WHERE (SELECT count() FROM ods.order_dist) > 0
    AND (SELECT count() FROM dds.order_v) > 0
    AND last_purchase_day IS NOT NULL
