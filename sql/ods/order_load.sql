-- Годные версии заказа. Срез читается по _load_id — тому же, что проставил
-- забор: разбор идёт по неизменной порции, а не по «всему, что появилось».
--
-- Разобранные значения берёт assumeNotNull: обнуляемый разбор стоит за
-- предикатом, который NULL уже отсёк, и приведение здесь не может упасть.

INSERT INTO ods.order_dist
{% include "ods/_order_wire_contract.sql" %}
SELECT
    JSONExtractString(raw, 'order_id') AS order_id,
    JSONExtract(raw, 'user_id', 'UInt64') AS user_id,
    JSONExtractString(raw, 'status') AS status,
    assumeNotNull(parseDateTime64InJodaSyntaxOrNull(
        JSONExtractString(raw, 'created_at'), ts_mask, 'UTC')) AS created_at,
    assumeNotNull(parseDateTime64InJodaSyntaxOrNull(
        JSONExtractString(raw, 'updated_at'), ts_mask, 'UTC')) AS updated_at,
    assumeNotNull(toDecimal64OrNull(
        JSONExtractString(raw, 'items_total'), 2)) AS items_total,
    assumeNotNull(toDecimal64OrNull(JSONExtractString(raw, 'discount'), 2)) AS discount,
    assumeNotNull(toDecimal64OrNull(JSONExtractString(raw, 'delivery'), 2)) AS delivery,
    assumeNotNull(toDecimal64OrNull(JSONExtractString(raw, 'total'), 2)) AS total,
    -- items кладётся сырым фрагментом JSON, а не разобранной структурой.
    JSONExtractRaw(raw, 'items') AS items,
    toDate(assumeNotNull(parseDateTimeInJodaSyntaxOrNull(
        JSONExtractString(raw, 'snapshot_date'),
        'yyyy-MM-dd', 'UTC'))) AS snapshot_date,
    -- Метки запуска и прибытия переносятся как есть. Поставь здесь now64(3) —
    -- и _load_ts молча ответила бы на другой вопрос: «когда разобрали».
    _load_id,
    _load_ts
FROM stg.orders_raw_dist
WHERE _load_id = {load_id:String} AND row_is_valid
SETTINGS distributed_foreground_insert = 1
