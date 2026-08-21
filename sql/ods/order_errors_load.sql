-- Брак: тот же срез и буквальное отрицание того же предиката.
--
-- Классы перекрываются, поэтому проверяются по порядку, а в error_class идёт
-- первый совпавший: скаляр проваливает и проверку на объект, и сверку ключей —
-- без объявленного порядка он попал бы то в один класс, то в другой.

INSERT INTO ods.order_errors_dist
{% include "ods/_order_wire_contract.sql" %}
SELECT
    raw,
    multiIf(
        NOT is_object, 'not_an_object',
        NOT keys_match, 'keyset_mismatch',
        'field_invalid'
    ) AS error_class,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    consumer_host,
    _load_id,
    _load_ts
FROM stg.orders_raw_dist
WHERE _load_id = {load_id:String} AND NOT row_is_valid
SETTINGS distributed_foreground_insert = 1
