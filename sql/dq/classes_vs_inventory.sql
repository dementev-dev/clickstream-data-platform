-- DQ: стендовая сторона технической растяжки от описи мира.
--
-- Эталонные счётчики читает Airflow из world-inventory.json: у настоящего
-- магазина такого оракула нет. Запрос возвращает только наблюдаемую сторону;
-- Python-задача сопоставляет ее с пятью счетчиками закрытого дня из описи.
SELECT
    order_day AS data_date,
    mismatch_class,
    count() AS orders
FROM dm.purchase_vs_orders_v
WHERE mismatch_class IN (
    'match',
    'cancelled',
    'lost_event',
    'duplicate_event',
    'amount_delta'
)
GROUP BY
    data_date,
    mismatch_class
ORDER BY
    data_date,
    mismatch_class
