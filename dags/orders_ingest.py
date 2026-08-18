"""Приём заказов: пакетный забор слепка из Kafka в хранилище.

Путь Kafka → STG → ODS у заказов один и живёт одним дагом: сначала забор
порции в сырьё, затем разбор того же среза в типизированный ODS и в таблицу
брака. Расписания у дага нет: его дёргает тот, кто положил слепок в топик, —
работник пульта мира после проигрыша дня, — и ждёт конца.

Контраст с приёмом событий и есть урок. События тянет матвью, навсегда
подписанная на чтеца: приём идёт сам, пока идёт поток. Слепок заказов
приезжает раз в модельный день целой выгрузкой, у которой есть начало и конец,
и забирает её запрос по команде. Push против pull — два режима на одном стенде,
каждый там, где ему место по природе источника.

Решения и доводы целиком — ADR 0008 (забор) и ADR 0010 (версии в ODS); форма
перехода — docs/architecture/orders/ingestion.md.
"""

from __future__ import annotations

import datetime
import logging

from airflow.sdk import Connection, dag, get_current_context, task

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

# Забор — один прямой SELECT, без цикла до пустоты: одна порция ClickHouse
# берёт десятки тысяч сообщений, а слепок дня — порядка полутора тысяч строк.
# Короткая порция оставит хвост до следующего прогона, а отказ после чтения
# унесёт прочитанное с собой: офсеты коммитятся в момент чтения. Граница
# целиком — ADR 0008, «Следствия».
#
# stream_like_engine_allow_direct_select разрешает читать чтеца запросом; вторая
# половина пары объявлена на самой таблице (sql/ddl/10-stg-tables.sql).
# distributed_foreground_insert = 1 — конвенция ETL-вставок стенда: задача не
# должна зеленеть раньше, чем строки легли на шарды.
TAKE_ONE_BATCH = """
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
"""

# Строгий приём: проверяется форма провода и ничего сверх неё.
#
# Ключей одиннадцать, и проверяются все: десять скалярных значений прямо
# образуют типизированную строку заказа. Внутрь items приём не смотрит —
# содержимое позиций, переходы статуса и равенства сумм остаются ниже границы
# (docs/architecture/orders/ingestion.md, «Граница строгого приёма»).
#
# Блок общий у обеих вставок нарочно: годная ветвь берёт row_is_valid, брак —
# буквальное NOT. Разойдись условия хоть на символ — строка либо задвоится,
# либо исчезнет молча.
#
# Отсюда два запрета на выражения предиката, и оба серьёзные. Первый: ни одно
# не возвращает NULL — трёхзначная логика дала бы строку, которую не берёт ни
# условие, ни его отрицание. Поэтому сравнения дают 0 или 1, а обнуляемый
# разбор заканчивается IS NOT NULL. Второй: ни одно не бросает исключений на
# произвольном raw — иначе одна грязная строка роняет весь переход, ради
# отсутствия чего таблица брака и заведена.
WIRE_CONTRACT = r"""
WITH
    -- Каноническое время провода: UTC, ровно три знака долей секунды.
    'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'' AS ts_mask,
    JSONType(raw) = 'Object' AS is_object,
    arraySort(JSONExtractKeys(raw)) = arraySort([
        'order_id', 'user_id', 'status', 'created_at', 'updated_at',
        'items_total', 'discount', 'delivery', 'total', 'items',
        'snapshot_date'
    ]) AS keys_match,
    JSONType(raw, 'order_id') = 'String'
        AND JSONType(raw, 'status') = 'String'
        -- Целое у JSONType зовётся двумя именами, и нужны оба: с одним
        -- Int64 законный идентификатор за 2^63 уехал бы в брак.
        AND JSONType(raw, 'user_id') IN ('Int64', 'UInt64')
        AND JSONExtract(raw, 'user_id', 'Nullable(UInt64)') IS NOT NULL
        AND JSONType(raw, 'items') = 'Array'
        -- Форму держит регулярка, реальность — разбор, и порознь они дырявы:
        -- маска берёт «2026-6-3T…» без ведущих нулей, а регулярка пропускает
        -- 30 февраля. Обе половины измерены — docs/architecture/storage.md,
        -- «Что проверено».
        AND arrayAll(k ->
                JSONType(raw, k) = 'String'
                AND match(JSONExtractString(raw, k),
                    '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$')
                AND parseDateTime64InJodaSyntaxOrNull(
                    JSONExtractString(raw, k), ts_mask, 'UTC') IS NOT NULL,
            ['created_at', 'updated_at'])
        -- Деньги — строка с ровно двумя знаками после точки. Регулярка держит
        -- форму, разбор — вместимость: у строки из двадцати девяток форма
        -- хороша, а в Decimal(18, 2) она не влезает.
        AND arrayAll(k ->
                JSONType(raw, k) = 'String'
                AND match(JSONExtractString(raw, k), '^\\d+\\.\\d{2}$')
                AND toDecimal64OrNull(JSONExtractString(raw, k), 2) IS NOT NULL,
            ['items_total', 'discount', 'delivery', 'total'])
        AND JSONType(raw, 'snapshot_date') = 'String'
        AND match(JSONExtractString(raw, 'snapshot_date'), '^\\d{4}-\\d{2}-\\d{2}$')
        AND parseDateTimeInJodaSyntaxOrNull(
            JSONExtractString(raw, 'snapshot_date'), 'yyyy-MM-dd', 'UTC') IS NOT NULL
        AS fields_valid,
    is_object AND keys_match AND fields_valid AS row_is_valid
"""

# Годные версии заказа. Срез читается по _load_id — тому же, что проставил
# забор: разбор идёт по неизменной порции, а не по «всему, что появилось».
#
# Разобранные значения берёт assumeNotNull: обнуляемый разбор стоит за
# предикатом, который NULL уже отсёк, и приведение здесь не может упасть.
PARSE_GOOD_ROWS = (
    "INSERT INTO ods.order_snapshot_dist"
    + WIRE_CONTRACT
    + r"""
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
"""
)

# Брак: тот же срез и буквальное отрицание того же предиката.
#
# Классы перекрываются, поэтому проверяются по порядку, а в error_class идёт
# первый совпавший: скаляр проваливает и проверку на объект, и сверку ключей —
# без объявленного порядка он попал бы то в один класс, то в другой.
PARSE_BAD_ROWS = (
    "INSERT INTO ods.order_snapshot_errors_dist"
    + WIRE_CONTRACT
    + r"""
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
"""
)


@dag(
    dag_id="orders_ingest",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    # Чтец у топика один, и группа потребителей у него одна. Два прогона разом
    # дрались бы за неё, а слепок разъехался бы по двум _load_id; второй
    # прогон подождёт своей очереди.
    max_active_runs=1,
    tags=["заказы"],
)
def orders_ingest():
    """Забрать приехавший слепок заказов и разложить его по слоям."""

    def clickhouse_client():
        # Импорт при выполнении задачи, а не при разборе файла: обработчик DAG
        # разбирает его снова и снова, и импорт наверху оплачивался бы каждым
        # разбором.
        import clickhouse_connect

        connection = Connection.get("clickhouse_default")
        return clickhouse_connect.get_client(
            host=connection.host,
            port=connection.port,
            username=connection.login,
            password=connection.password,
            database=connection.schema or "default",
            connect_timeout=5,
            send_receive_timeout=30,
        )

    @task
    def pull_batch() -> None:
        load_id = get_current_context()["run_id"]
        client = clickhouse_client()
        try:
            summary = client.command(TAKE_ONE_BATCH, parameters={"load_id": load_id})
        finally:
            client.close()
        # Размер порции — read_rows: written_rows у вставки в Distributed
        # считает не приехавшее.
        logging.info(
            "порция принята: строк %s, _load_id %s",
            summary.summary["read_rows"],
            load_id,
        )

    @task
    def parse_batch() -> None:
        """Разобрать срез сырья в версии заказов и в брак.

        Обе вставки в одном task_id: транзакции между ними ClickHouse не даёт,
        а повтор задачи безопасен — срез читается по тому же неизменному
        _load_id (docs/architecture/orders/ingestion.md, «Поток данных»).
        """
        load_id = get_current_context()["run_id"]
        client = clickhouse_client()
        try:
            client.command(PARSE_GOOD_ROWS, parameters={"load_id": load_id})
            client.command(PARSE_BAD_ROWS, parameters={"load_id": load_id})
        finally:
            client.close()
        # Счётчиков строк нет: у запроса с WHERE read_rows считает прочитанное
        # с диска, а не подошедшее (storage.md, «Что проверено»).
        logging.info("срез разобран: _load_id %s", load_id)

    pull_batch() >> parse_batch()


orders_ingest()
