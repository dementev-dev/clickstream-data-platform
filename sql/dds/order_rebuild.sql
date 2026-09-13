-- Сборка выбранных дней в донор перед заменой партиций DDS.
-- Донор очищается в начале: иначе новые строки добавились бы к прежним.
-- После сборки он остается для диагностики до следующего запуска.
--
-- DROP PARTITION ALL доступен роли etl через ALTER; отдельного права
-- TRUNCATE у нее нет (ADR 0007). Весь отрезок дней собирается одной вставкой.

ALTER TABLE dds.order_stage_rep ON CLUSTER clickstream_cluster DROP PARTITION ALL;

INSERT INTO dds.order_stage_dist
(
    order_id,
    user_id,
    status,
    order_date,
    created_at,
    updated_at,
    items_total,
    discount,
    delivery,
    total,
    item_sku,
    item_quantity,
    item_price,
    _load_id,
    _load_ts
)
-- Разбор позиций из JSON: цена в строке сразу превращается в Decimal.
-- ODS проверил только тип массива items. Неверное содержимое здесь может
-- дать значения по умолчанию: qty вида "oops" и пропущенная цена дают 0,
-- скаляр вместо объекта выпадает из массива. Наблюдения —
-- docs/architecture/storage.md, «Что проверено».
-- Проверку суммы позиций участник добавляет в docs/labs/5-1-order-checks.md.

WITH JSONExtract(
        items, 'Array(Tuple(sku String, qty UInt32, price Decimal(18, 2)))'
    ) AS lines
SELECT
    order_id,
    user_id,
    status,
    -- День и время в Europe/Samara: витрины используют готовые колонки.
    toDate(toTimeZone(created_at, 'Europe/Samara')),
    toTimeZone(created_at, 'Europe/Samara'),
    toTimeZone(updated_at, 'Europe/Samara'),
    items_total,
    discount,
    delivery,
    total,
    arrayMap(line -> line.sku, lines),
    arrayMap(line -> line.qty, lines),
    arrayMap(line -> line.price, lines),
    -- Метки этой сборки DDS, общие для всех вставляемых строк.
    {load_id:String},
    now64(3, 'UTC')
FROM ods.order_v
WHERE toDate(toTimeZone(created_at, 'Europe/Samara'))
        BETWEEN {first_day:Date} AND {last_day:Date}
    -- Дополнительный фильтр сокращает чтение партиций ODS по UTC.
    -- Модельные сутки Europe/Samara пересекают две даты UTC, поэтому
    -- левая граница берется на день раньше. Точный отбор — условием выше.
    AND toDate(created_at) BETWEEN {first_day:Date} - 1 AND {last_day:Date}
-- Ждем доставки на шарды и распределяем строки по ключу цели
-- (docs/architecture/storage.md, «Раскладка по шардам»).
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
