-- Разбор одного среза заказов: сначала годные версии, затем брак.
--
-- Обе вставки должны делить один предикат. Локальный макрос Jinja
-- разворачивает его в каждое выражение до того, как оператор разделит файл по
-- точкам с запятой. Так контракт остаётся рядом с запросами и не становится
-- третьим SQL-файлом, который нельзя исполнить самостоятельно.
--
-- Строгий приём проверяет форму провода и ничего сверх неё. Ключей одиннадцать,
-- и проверяются все: десять скалярных значений прямо образуют типизированную
-- строку заказа. Внутрь items приём не смотрит — содержимое позиций, переходы
-- статуса и равенства сумм остаются ниже этой границы
-- (docs/architecture/orders/ingestion.md, «Граница строгого приёма»).
--
-- У общего предиката два обязательных свойства. Он всегда даёт 0 или 1, не
-- NULL: иначе строку не возьмут ни условие, ни его отрицание. И он не бросает
-- исключений на произвольном raw: одна грязная строка не должна ронять весь
-- переход, ради этого и существует таблица брака.

{% macro order_wire_contract() %}
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
{% endmacro %}

-- Годные версии заказа. Срез читается по _load_id — тому же, что проставил
-- забор: разбор идёт по неизменной порции, а не по «всему, что появилось».
--
-- Разобранные значения берёт assumeNotNull: обнуляемый разбор стоит за
-- предикатом, который NULL уже отсёк, и приведение здесь не может упасть.

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
-- Заводская двойка parallel_distributed_insert_select исполняет вставку из
-- Distributed в Distributed локально на каждом шарде, минуя ключ шардирования
-- цели, — так заказы и расползлись по обоим шардам. Ноль отдаёт раскладку
-- инициатору, по ключу цели. Капкан целиком — docs/architecture/storage.md,
-- «Раскладка по шардам».
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;

-- Брак: тот же срез и буквальное отрицание того же предиката.
--
-- Классы перекрываются, поэтому проверяются по порядку, а в error_class идёт
-- первый совпавший: скаляр проваливает и проверку на объект, и сверку ключей —
-- без объявленного порядка он попал бы то в один класс, то в другой.

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
-- Тот же ноль: ключ брака сегодня совпадает с ключом STG, и локальная
-- запись легла бы верно — но по совпадению, а не по контракту.
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
