-- DQ: дневная сводка брака против живого окна таблиц ошибок ODS.
--
-- Граница окна повторяет TTL источников; более старую историю сверять не с чем.

WITH
    reference AS
    (
        SELECT
            load_date,
            source,
            error_class,
            count() AS error_rows
        FROM
        (
            SELECT
                toDate(_load_ts) AS load_date,
                'event' AS source,
                error_class
            FROM ods.event_errors_dist
            UNION ALL
            SELECT
                toDate(_load_ts) AS load_date,
                'order' AS source,
                error_class
            FROM ods.order_errors_dist
        )
        WHERE load_date >= toDate(now() - INTERVAL 1 MONTH)
        GROUP BY load_date, source, error_class
    ),
    actual AS
    (
        SELECT
            load_date,
            source,
            error_class,
            count() AS physical_rows,
            any(error_rows) AS error_rows
        FROM dm.dq_errors_daily_v
        WHERE load_date >= toDate(now() - INTERVAL 1 MONTH)
        GROUP BY load_date, source, error_class
    ),
    comparison AS
    (
        SELECT
            coalesce(reference.load_date, actual.load_date) AS data_date,
            concat(
                coalesce(reference.source, actual.source),
                '/',
                coalesce(reference.error_class, actual.error_class)
            ) AS business_key,
            isNotNull(reference.source) AS reference_present,
            isNotNull(actual.source) AS actual_present,
            reference.error_rows AS reference_value,
            actual.error_rows AS actual_value,
            actual.physical_rows AS physical_rows,
            isNull(reference.source)
                OR isNull(actual.source)
                OR actual.physical_rows != 1
                OR NOT isNotDistinctFrom(reference.error_rows, actual.error_rows)
                AS failed
        FROM reference
        GLOBAL FULL OUTER JOIN actual USING (load_date, source, error_class)
    )
SELECT
    data_date,
    countIf(reference_present) AS reference_rows,
    countIf(actual_present) AS actual_rows,
    countIf(failed) AS failed_rows,
    groupArrayIf(20)(
        concat(
            toString(data_date),
            ' source/error_class=',
            business_key,
            ': источник=',
            if(reference_present, toString(reference_value), 'нет ключа'),
            ', витрина=',
            if(
                actual_present,
                concat(toString(actual_value), ', rows=', toString(physical_rows)),
                'нет ключа'
            )
        ),
        failed
    ) AS diagnostics
FROM comparison
GROUP BY data_date
ORDER BY data_date
SETTINGS join_use_nulls = 1
