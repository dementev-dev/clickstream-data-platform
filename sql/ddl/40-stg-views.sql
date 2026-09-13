-- Прием событий из Kafka в STG.
-- Подключение этого представления запускает чтение топика. Поэтому файл 40
-- применяется после таблиц и представлений разбора из файлов 20 и 30:
-- иначе часть сообщений сохранится в STG, но не попадет в ODS.
--
-- Запись через stg.hits_raw_dist распределяет сообщения по ключу таблицы.
-- Она фоновая; возможную потерю при сбое описывает раздел «Приём»
-- в docs/architecture/storage.md.
--
-- Сведения о доставке вычисляются здесь, на ноде чтения Kafka.
-- DEFAULT hostName() только в локальной таблице назвал бы шард-получатель.
-- Порядок SELECT совпадает с порядком колонок цели.

CREATE MATERIALIZED VIEW IF NOT EXISTS stg.hits_raw_mv ON CLUSTER clickstream_cluster
TO stg.hits_raw_dist
AS
SELECT
    raw,
    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    _timestamp_ms AS kafka_timestamp,
    hostName() AS consumer_host,
    now64(3) AS _load_ts
FROM stg.hits_raw_kafka;
