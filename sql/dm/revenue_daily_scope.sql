-- DM: объем пересборки выручки.
--
-- Последний слепок ODS называет дни, состояние которых источник еще может
-- изменить. Сам агрегат читает только DDS; ODS здесь нужен лишь для границ.
-- Первый запуск и повтор после частичного отказа расширяют левую границу до
-- первого дня оплаченных заказов, которого еще нет в витрине.
--
-- Файл кончается без точки с запятой: даг читает его одним запросом и ждет
-- одну строку с границами.
WITH
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
        SELECT minOrNull(report_date)
        FROM (SELECT DISTINCT order_date AS report_date FROM dds.order_v WHERE status = 'paid')
        -- Правая сторона тоже распределенная: GLOBAL собирает множество дней
        -- на инициаторе и не превращает запрос в запрещенное двойное чтение.
        WHERE report_date GLOBAL NOT IN (
            SELECT report_date FROM dm.revenue_daily_dist
        )
    ) AS first_missing_day
SELECT
    least(window_start, coalesce(first_missing_day, window_start)) AS first_day,
    window_end AS last_day
WHERE (SELECT count() FROM dds.order_v) > 0
    AND (SELECT count() FROM ods.order_dist) > 0
