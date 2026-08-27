-- DQ: обогащенные события против прямого ответа DDS за один день.
--
-- День передает даг, чтобы широкая сверка не собирала всю историю в памяти.

WITH
    reference AS
    (
        SELECT
            event_date,
            event_id,
            tuple(
                event_time,
                toHour(event_time),
                client_id,
                event_type,
                url,
                path(url),
                referer,
                domain(referer),
                title,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_content,
                utm_term,
                device_category,
                browser,
                operating_system,
                region_country,
                region_city,
                product_id,
                product_name,
                product_category,
                product_price,
                product_quantity,
                product_event_type,
                purchase_id,
                purchase_revenue,
                purchase_coupon
            ) AS value
        FROM dds.event_v
        WHERE event_date = {day:Date}
    ),
    actual AS
    (
        SELECT
            event_date,
            event_id,
            count() AS physical_rows,
            any(tuple(
                event_time,
                event_hour,
                client_id,
                event_type,
                url,
                page_path,
                referer,
                referer_host,
                title,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_content,
                utm_term,
                device_category,
                browser,
                operating_system,
                region_country,
                region_city,
                product_id,
                product_name,
                product_category,
                product_price,
                product_quantity,
                product_event_type,
                purchase_id,
                purchase_revenue,
                purchase_coupon
            )) AS value
        FROM dm.events_enriched_v
        WHERE event_date = {day:Date}
        GROUP BY event_date, event_id
    ),
    comparison AS
    (
        SELECT
            coalesce(reference.event_date, actual.event_date) AS data_date,
            coalesce(reference.event_id, actual.event_id) AS business_key,
            isNotNull(reference.event_id) AS reference_present,
            isNotNull(actual.event_id) AS actual_present,
            reference.value AS reference_value,
            actual.value AS actual_value,
            actual.physical_rows AS physical_rows,
            isNull(reference.event_id)
                OR isNull(actual.event_id)
                OR actual.physical_rows != 1
                OR NOT isNotDistinctFrom(reference.value, actual.value) AS failed
        FROM reference
        GLOBAL FULL OUTER JOIN actual USING (event_date, event_id)
    )
SELECT
    data_date,
    countIf(reference_present) AS reference_rows,
    countIf(actual_present) AS actual_rows,
    countIf(failed) AS failed_rows,
    groupArrayIf(20)(
        concat(
            toString(data_date),
            ' event_id=',
            toString(business_key),
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
