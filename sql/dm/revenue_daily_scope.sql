-- DM: диапазон пересборки выручки, first_day и last_day включительно.
-- Последний слепок ODS задает даты оформления заказов, которые еще могут
-- измениться. Саму выручку считаем по DDS; ODS нужен только для выбора дат.
-- Левая граница расширяется до первого дня с оплаченными заказами DDS,
-- которого нет в витрине. Так первый запуск охватывает историю выручки.
-- По наличию даты нельзя обнаружить частично заполненный день.
--
-- Точки с запятой в конце нет: драйвер добавляет FORMAT к этому запросу.
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
        -- GLOBAL собирает список дней витрины со всех шардов.
        WHERE report_date GLOBAL NOT IN (
            SELECT report_date FROM dm.revenue_daily_dist
        )
    ) AS first_missing_day
SELECT
    least(window_start, coalesce(first_missing_day, window_start)) AS first_day,
    window_end AS last_day
-- Для выбора диапазона нужны заказы в ODS, для расчета — в DDS.
-- Эта проверка наличия строк не подтверждает полноту источников.
WHERE (SELECT count() FROM dds.order_v) > 0
    AND (SELECT count() FROM ods.order_dist) > 0
