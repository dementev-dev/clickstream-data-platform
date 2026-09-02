-- STG: чтецы топиков hits и orders, таблицы STG обоих источников.
--
-- Сырой слой ничего не интерпретирует: сообщение ложится строкой, как пришло,
-- рядом с метаданными доставки. Довод целиком — ADR 0005, конвенции колонок и
-- сроков — docs/architecture/storage.md.

-- Чтец топика. Колонка ровно одна: формат RawBLOB читает вход в одно значение
-- и рассчитан на таблицу с единственным полем String, метаданные доставки
-- берутся только из виртуальных колонок, своего к чтецу добавить нельзя.
-- Проверено на стенде 5 августа 2026 года: одно непустое сообщение Kafka даёт
-- ровно одну строку, сообщения в одной пачке продюсера не склеиваются. Граница
-- у обещания есть: запись с пустым значением и запись-надгробие читаются,
-- двигают офсет и строки не дают вовсе. Свойство принято осознанно — генератор
-- таких сообщений не шлёт; замер и довод — в доке хранилища, «Что проверено».
--
-- Чтец стоит на обеих нодах и читает одной группой потребителей. Имя группы
-- одинаково на обеих по построению: DDL идёт ON CLUSTER и макросов в имени
-- нет. Разные группы дали бы каждой ноде полную копию топика.
--
-- Читать топик движок начинает не сейчас, а в момент создания матвью
-- (40-stg-views.sql) — см. комментарий там.
CREATE TABLE IF NOT EXISTS stg.hits_raw_kafka ON CLUSTER clickstream_cluster
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'hits',
    kafka_group_name = 'clickstream_hits',
    kafka_format = 'RawBLOB';

-- Локальная таблица STG.
--
-- Движок именно ReplicatedMergeTree, не Replacing: повтор доставки в STG
-- обязан быть виден — ради этого слой и заведён.
--
-- Служебные колонки не повторяют имён виртуальных (_topic, _partition,
-- _offset, _timestamp у Kafka), иначе в матвью перестанет читаться, что дано
-- движком, а что положено нами. consumer_host — имя читавшей топик ноды:
-- виртуальные колонки его не несут, а после записи в Distributed он уже
-- невосстановим.
--
-- kafka_timestamp — Nullable(DateTime64(3, 'UTC')), и заполняется из виртуальной
-- колонки _timestamp_ms, а не из _timestamp. Измерено на стенде 5 августа
-- 2026 года: _timestamp — Nullable(DateTime), то есть секунды; _timestamp_ms —
-- Nullable(DateTime64(3)). Взяты миллисекунды: у брокера метка миллисекундная,
-- _load_ts рядом тоже миллисекундная, а сырой слой хранит то, что приехало, и
-- округлять ему нечего. Обнуляемость обязательна: метку брокер заполняет не
-- всегда, а необнуляемый тип дал бы либо падение приёма, либо тихий 1970 год.
--
-- Пояс у обеих меток написан в типе. Хранимого числа он не меняет, а решает,
-- в какие сутки метка попадёт, — то есть чем окажется toDate(_load_ts) в ключе
-- партиции ниже. Не напиши его — пояс возьмётся у сервера, а это умолчание в
-- коде не видно. Правило целиком — docs/architecture/storage.md, «Часовые
-- пояса».
--
-- Нарезка и срок жизни — по _load_ts, то есть по реальному времени загрузки:
-- модельный день события живёт в ODS, а по нему TTL был бы просто сломан.
-- Срок — трое суток плюс хвост до суток: куски снимаются целиком
-- (ttl_only_drop_parts), а партицию закрывает календарный день. Значение
-- настройки проставлено явно, чтобы поведение не зависело от умолчания версии.
--
-- Путь в keeper — с базой и без {uuid}: одноимённые таблицы разных слоёв иначе
-- подерутся за один узел, а читаемый путь на учебном стенде сам по себе
-- половина урока про keeper.
CREATE TABLE IF NOT EXISTS stg.hits_raw_rep ON CLUSTER clickstream_cluster
(
    raw String,
    kafka_topic LowCardinality(String),
    kafka_partition UInt64,
    kafka_offset UInt64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),
    consumer_host LowCardinality(String),
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toDate(_load_ts)
ORDER BY (kafka_partition, kafka_offset)
TTL toDateTime(_load_ts) + INTERVAL 3 DAY
SETTINGS ttl_only_drop_parts = 1;

-- Лицо слоя: пишем и читаем через него, локальная таблица остаётся для
-- обслуживания. Ключ шардирования — хеш сырой строки: разложить строку иначе
-- нечем, зато одинаковые сообщения ложатся на один шард.
--
-- Операции с партициями по этой таблице не работают: проверено на стенде
-- 5 августа 2026 года, и DROP PARTITION, и REPLACE PARTITION отвечают
-- кодом 48 «Table engine Distributed doesn't support partitioning». Партиции
-- снимаются по локальным таблицам, ON CLUSTER.
CREATE TABLE IF NOT EXISTS stg.hits_raw_dist ON CLUSTER clickstream_cluster
AS stg.hits_raw_rep
ENGINE = Distributed('clickstream_cluster', 'stg', 'hits_raw_rep', cityHash64(raw));

-- Чтец топика orders. Матвью к нему не привязана: слепок забирает прямым
-- SELECT даг orders_ingest. Почему пулл, почему без матвью и почему у топика
-- одна партиция — ADR 0008; сам топик заранее создаёт world_initialize.
--
-- Без ON CLUSTER: таблица нужна только на clickhouse-01 — той ноде, к которой
-- у Airflow подключение, и она же одна читает топик.
--
-- kafka_commit_on_select — вторая настройка прямого чтения: без неё офсеты не
-- коммитятся и каждый запуск забирает один и тот же слепок заново. Первая,
-- stream_like_engine_allow_direct_select, живёт на уровне запроса и стоит в
-- самом заборе (dags/orders_ingest.py).
CREATE TABLE IF NOT EXISTS stg.orders_raw_kafka
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickstream_orders',
    kafka_format = 'RawBLOB',
    kafka_commit_on_select = 1;

-- Локальная таблица STG заказов. Колонки, типы, ключ, нарезка и срок жизни —
-- те же, что у STG событий, и по тем же доводам: docs/architecture/storage.md.
--
-- Своя колонка здесь одна — _load_id, идентификатор пачки загрузки: он равен
-- run_id прогона Airflow, который забрал порцию, и по нему разбор в ODS читает
-- неизменный срез.
CREATE TABLE IF NOT EXISTS stg.orders_raw_rep ON CLUSTER clickstream_cluster
(
    raw String,
    kafka_topic LowCardinality(String),
    kafka_partition UInt64,
    kafka_offset UInt64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),
    consumer_host LowCardinality(String),
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toDate(_load_ts)
ORDER BY (kafka_partition, kafka_offset)
TTL toDateTime(_load_ts) + INTERVAL 3 DAY
SETTINGS ttl_only_drop_parts = 1;

-- Лицо слоя: пакетный забор пишет сюда, а не в локальную таблицу. Раскладку по
-- шардам обязан решать ключ шардирования, то есть свойство данных, а не то,
-- какая нода выполняла запрос, — а при пулле она всегда одна и та же.
CREATE TABLE IF NOT EXISTS stg.orders_raw_dist ON CLUSTER clickstream_cluster
AS stg.orders_raw_rep
ENGINE = Distributed('clickstream_cluster', 'stg', 'orders_raw_rep', cityHash64(raw));
