-- DQ: эффективность UTM-меток против прямого агрегата событий DDS.
--
-- Полное объединение не прячет набор меток, исчезнувший с любой стороны.

WITH
    reference AS
    (
        SELECT
            event_date AS report_date,
            utm_source,
            utm_medium,
            utm_campaign,
            tuple(
                uniqExact(client_id),
                countIf(event_type = 'pageview'),
                countIf(event_type = 'add_to_cart'),
                countIf(event_type = 'purchase'),
                sumIf(purchase_revenue[1], event_type = 'purchase')
            ) AS value
        FROM dds.event_v
        GROUP BY report_date, utm_source, utm_medium, utm_campaign
    ),
    actual AS
    (
        SELECT
            report_date,
            utm_source,
            utm_medium,
            utm_campaign,
            count() AS physical_rows,
            any(tuple(
                visitors,
                pageviews,
                add_to_carts,
                purchases,
                declared_revenue
            )) AS value
        FROM dm.utm_effectiveness_v
        GROUP BY report_date, utm_source, utm_medium, utm_campaign
    ),
    comparison AS
    (
        SELECT
            coalesce(reference.report_date, actual.report_date) AS data_date,
            concat(
                'utm_source=', coalesce(reference.utm_source, actual.utm_source),
                ', utm_medium=', coalesce(reference.utm_medium, actual.utm_medium),
                ', utm_campaign=',
                coalesce(reference.utm_campaign, actual.utm_campaign)
            ) AS business_key,
            isNotNull(reference.utm_source) AS reference_present,
            isNotNull(actual.utm_source) AS actual_present,
            reference.value AS reference_value,
            actual.value AS actual_value,
            actual.physical_rows AS physical_rows,
            isNull(reference.utm_source)
                OR isNull(actual.utm_source)
                OR actual.physical_rows != 1
                OR NOT isNotDistinctFrom(reference.value, actual.value) AS failed
        FROM reference
        GLOBAL FULL OUTER JOIN actual
            USING (report_date, utm_source, utm_medium, utm_campaign)
    )
SELECT
    data_date,
    countIf(reference_present) AS reference_rows,
    countIf(actual_present) AS actual_rows,
    countIf(failed) AS failed_rows,
    groupArrayIf(20)(
        concat(
            toString(data_date),
            ' ',
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
