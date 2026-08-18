-- Форма провода одной строки слепка: общая часть обеих вставок ODS.
--
-- Строгий приём: проверяется форма провода и ничего сверх неё.
--
-- Ключей одиннадцать, и проверяются все: десять скалярных значений прямо
-- образуют типизированную строку заказа. Внутрь items приём не смотрит —
-- содержимое позиций, переходы статуса и равенства сумм остаются ниже границы
-- (docs/architecture/orders/ingestion.md, «Граница строгого приёма»).
--
-- Файл включают обе вставки нарочно: годная ветвь берёт row_is_valid, брак —
-- буквальное NOT. Разойдись условия хоть на символ — строка либо задвоится,
-- либо исчезнет молча.
--
-- Отсюда два запрета на выражения предиката, и оба серьёзные. Первый: ни одно
-- не возвращает NULL — трёхзначная логика дала бы строку, которую не берёт ни
-- условие, ни его отрицание. Поэтому сравнения дают 0 или 1, а обнуляемый
-- разбор заканчивается IS NOT NULL. Второй: ни одно не бросает исключений на
-- произвольном raw — иначе одна грязная строка роняет весь переход, ради
-- отсутствия чего таблица брака и заведена.

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
