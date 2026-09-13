-- Разбор сохраненной порции заказов: сначала годные версии, затем ошибки.
-- Макрос Jinja подставляет одно условие годности в обе вставки.
-- Вторая вставка берет его отрицание, чтобы каждая строка получила результат.
-- Условие должно давать только 0 или 1 и не вызывать исключений на неверном raw.
--
-- Проверяются одиннадцать полей верхнего уровня. Внутри items поля не проверяются;
-- позиции разбирает DDS, а равенства сумм проверяются отдельно.
-- Точные правила — docs/architecture/orders/ingestion.md, «Граница строгого приёма».

{% macro order_wire_contract() %}
WITH
    -- Время источника: UTC, ровно три знака долей секунды.
    'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'' AS ts_mask,
    JSONType(raw) = 'Object' AS is_object,
    arraySort(JSONExtractKeys(raw)) = arraySort([
        'order_id', 'user_id', 'status', 'created_at', 'updated_at',
        'items_total', 'discount', 'delivery', 'total', 'items',
        'snapshot_date'
    ]) AS keys_match,
    JSONType(raw, 'order_id') = 'String'
        AND JSONType(raw, 'status') = 'String'
        -- JSONType различает Int64 и UInt64; принимаются оба, если значение
        -- разбирается в UInt64, в том числе идентификаторы больше 2^63−1.
        AND JSONType(raw, 'user_id') IN ('Int64', 'UInt64')
        AND JSONExtract(raw, 'user_id', 'Nullable(UInt64)') IS NOT NULL
        AND JSONType(raw, 'items') = 'Array'
        -- Регулярное выражение проверяет точную форму, включая ведущие нули.
        -- Разбор проверяет само время: например, отвергает 30 февраля.
        -- Замеры функций — docs/architecture/storage.md, «Что проверено».
        AND arrayAll(k ->
                JSONType(raw, k) = 'String'
                AND match(JSONExtractString(raw, k),
                    '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$')
                AND parseDateTime64InJodaSyntaxOrNull(
                    JSONExtractString(raw, k), ts_mask, 'UTC') IS NOT NULL,
            ['created_at', 'updated_at'])
        -- Сумма в рублях: строка с двумя знаками после точки, без минуса.
        -- После проверки формы проверяется возможность разбора в Decimal.
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
{% endmacro %}

-- Годные строки сохраненного среза по _load_id запуска приема.
-- WHERE исключает строки с NULL в разобранных полях; assumeNotNull
-- приводит оставшиеся значения к типам целевой таблицы.

INSERT INTO ods.order_dist
{{ order_wire_contract() }}
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
    -- items сохраняется строкой JSON для разбора позиций в DDS.
    JSONExtractRaw(raw, 'items') AS items,
    toDate(assumeNotNull(parseDateTimeInJodaSyntaxOrNull(
        JSONExtractString(raw, 'snapshot_date'),
        'yyyy-MM-dd', 'UTC'))) AS snapshot_date,
    -- _load_ts остается временем приема в STG; _load_id связывает с исходной порцией.
    _load_id,
    _load_ts
FROM stg.orders_raw_dist
WHERE _load_id = {load_id:String} AND row_is_valid
-- Ноль заставляет распределить результат по ключу цели. При значении 2
-- вставка может сохранить раскладку STG, где ключ — raw, вместо order_id.
-- Подробнее — docs/architecture/storage.md, «Раскладка по шардам».
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;

-- Ошибки: тот же срез и отрицание условия годности.
-- Класс определяется первым нарушенным условием: например, JSON-строка
-- не проходит и проверку объекта, и проверку ключей, но получает not_an_object.

INSERT INTO ods.order_errors_dist
{{ order_wire_contract() }}
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
-- Та же настройка: распределяем строки по ключу целевой таблицы ошибок.
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
