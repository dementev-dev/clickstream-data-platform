-- Порция сообщений заказов из Kafka в STG, без разбора JSON.
-- Один SELECT читает одну порцию; непрочитанное остается до следующего запуска.
-- Полнота слепка не проверяется. Если после чтения вставка упадет, подтвержденные
-- сообщения уже не перечитаются (docs/architecture/orders/ingestion.md).
--
-- stream_like_engine_allow_direct_select разрешает прямое чтение Kafka;
-- kafka_commit_on_select включен в sql/ddl/10-stg-tables.sql.
-- distributed_foreground_insert = 1 ждет записи на шарды до завершения задачи.

INSERT INTO stg.orders_raw_dist
SELECT
    raw,
    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    _timestamp_ms AS kafka_timestamp,
    hostName() AS consumer_host,
    {load_id:String} AS _load_id,
    now64(3) AS _load_ts
FROM stg.orders_raw_kafka
SETTINGS
    stream_like_engine_allow_direct_select = 1,
    distributed_foreground_insert = 1
