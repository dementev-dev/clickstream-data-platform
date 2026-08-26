-- DDS: полный пересчёт известных пар куки с пользователем.
--
-- События разложены по client_id; заказы по order_id. Ключи соединения не
-- ко-локированы, поэтому правило слоя требует явный GLOBAL. С подзапросами с
-- обеих сторон ClickHouse 26.3 выполняет соединение один раз на инициаторе
-- (docs/research/2026-08-22-distributed-join.md).

INSERT INTO dds.identity_map_dist
(
    client_id,
    user_id,
    _load_id,
    _load_ts
)
SELECT
    client_id,
    user_id,
    {load_id:String},
    now64(3, 'UTC')
FROM
(
    SELECT DISTINCT
        purchase.client_id,
        orders.user_id
    FROM
    (
        SELECT
            client_id,
            -- По договору события purchase_id содержит ровно один номер
            -- заказа: одно подтверждение оформляет один заказ (спека, раздел 4).
            purchase_id[1] AS order_id
        FROM dds.event_v
        WHERE event_type = 'purchase'
    ) AS purchase
    GLOBAL INNER JOIN
    (
        SELECT
            order_id,
            user_id
        FROM dds.order_v
    ) AS orders ON purchase.order_id = orders.order_id
)
-- В этой форме ClickHouse 26.3 уже пишет через инициатор. Явный ноль фиксирует
-- раскладку по ключу цели, если форма запроса или план изменятся (капкан #137).
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
