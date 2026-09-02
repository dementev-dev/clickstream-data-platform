-- Забор порции слепка заказов из Kafka в STG.
--
-- Забор — один прямой SELECT, без цикла до пустоты: одна порция ClickHouse
-- берёт десятки тысяч сообщений, а слепок дня — порядка полутора тысяч строк.
-- Короткая порция оставит хвост до следующего прогона, а отказ после чтения
-- унесёт прочитанное с собой: офсеты коммитятся в момент чтения. Граница
-- целиком — ADR 0008, «Следствия».
--
-- stream_like_engine_allow_direct_select разрешает читать чтеца запросом; вторая
-- половина пары объявлена на самой таблице (sql/ddl/10-stg-tables.sql).
-- distributed_foreground_insert = 1 — конвенция ETL-вставок стенда: задача не
-- должна зеленеть раньше, чем строки легли на шарды.

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
