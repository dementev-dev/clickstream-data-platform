-- DQ: дневная популярность страниц против прямого агрегата событий DDS.
--
-- Полное объединение не прячет страницу, исчезнувшую с любой стороны.

WITH
    reference AS
    (
        SELECT
            event_date AS report_date,
            path(url) AS page_path,
            tuple(
                countIf(event_type = 'pageview'),
                uniqExact(client_id),
                countIf(event_type = 'add_to_cart'),
                countIf(event_type = 'purchase')
            ) AS value
        FROM dds.event_v
        GROUP BY report_date, page_path
    ),
    actual AS
    (
        SELECT
            report_date,
            page_path,
            count() AS physical_rows,
            any(tuple(pageviews, visitors, cart_adds, purchases)) AS value
        FROM dm.top_pages_daily_v
        GROUP BY report_date, page_path
    ),
    comparison AS
    (
        SELECT
            coalesce(reference.report_date, actual.report_date) AS data_date,
            coalesce(reference.page_path, actual.page_path) AS business_key,
            isNotNull(reference.page_path) AS reference_present,
            isNotNull(actual.page_path) AS actual_present,
            reference.value AS reference_value,
            actual.value AS actual_value,
            actual.physical_rows AS physical_rows,
            isNull(reference.page_path)
                OR isNull(actual.page_path)
                OR actual.physical_rows != 1
                OR NOT isNotDistinctFrom(reference.value, actual.value) AS failed
        FROM reference
        GLOBAL FULL OUTER JOIN actual USING (report_date, page_path)
    )
SELECT
    data_date,
    countIf(reference_present) AS reference_rows,
    countIf(actual_present) AS actual_rows,
    countIf(failed) AS failed_rows,
    groupArrayIf(20)(
        concat(
            toString(data_date),
            ' page_path=',
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
