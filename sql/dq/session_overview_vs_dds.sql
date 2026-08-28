-- DQ: дневной портрет сессий против прямого агрегата сессий DDS.
--
-- Полное объединение не прячет день или устройство, исчезнувшие с любой стороны.

WITH
    reference AS
    (
        SELECT
            session_date AS report_date,
            device_category,
            tuple(
                count(),
                uniqExact(client_id),
                countIf(events_total = 1),
                countIf(purchases > 0),
                sum(duration_seconds),
                sum(events_total),
                sum(pageviews),
                sum(cart_adds),
                sum(purchases)
            ) AS value
        FROM dds.session_v
        GROUP BY report_date, device_category
    ),
    actual AS
    (
        SELECT
            report_date,
            device_category,
            count() AS physical_rows,
            any(tuple(
                sessions,
                visitors,
                bounce_sessions,
                purchase_sessions,
                duration_seconds_total,
                events_total,
                pageviews,
                cart_adds,
                purchases
            )) AS value
        FROM dm.session_overview_v
        GROUP BY report_date, device_category
    ),
    comparison AS
    (
        SELECT
            coalesce(reference.report_date, actual.report_date) AS data_date,
            coalesce(reference.device_category, actual.device_category)
                AS business_key,
            isNotNull(reference.device_category) AS reference_present,
            isNotNull(actual.device_category) AS actual_present,
            reference.value AS reference_value,
            actual.value AS actual_value,
            actual.physical_rows AS physical_rows,
            isNull(reference.device_category)
                OR isNull(actual.device_category)
                OR actual.physical_rows != 1
                OR NOT isNotDistinctFrom(reference.value, actual.value) AS failed
        FROM reference
        GLOBAL FULL OUTER JOIN actual USING (report_date, device_category)
    )
SELECT
    data_date,
    countIf(reference_present) AS reference_rows,
    countIf(actual_present) AS actual_rows,
    countIf(failed) AS failed_rows,
    groupArrayIf(20)(
        concat(
            toString(data_date),
            ' device_category=',
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
