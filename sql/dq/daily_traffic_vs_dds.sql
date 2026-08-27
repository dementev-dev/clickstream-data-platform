-- DQ: дневной трафик против прямого агрегата сессий и карты идентичностей.
--
-- Публичные представления скрывают физическую ко-локацию, поэтому GLOBAL
-- назван явно, как в самой витрине. Полное объединение не прячет день,
-- исчезнувший с любой стороны.
WITH
    reference AS
    (
        SELECT
            sessions.session_date AS report_date,
            uniqExact(sessions.client_id) AS visitors,
            uniqExactIf(identities.user_id, identities.user_id != 0) AS known_users
        FROM dds.session_v AS sessions
        GLOBAL LEFT JOIN dds.identity_map_v AS identities USING (client_id)
        GROUP BY report_date
    ),
    actual AS
    (
        SELECT
            report_date,
            count() AS physical_rows,
            any(visitors) AS visitors,
            any(known_users) AS known_users
        FROM dm.daily_traffic_v
        GROUP BY report_date
    )
SELECT
    coalesce(reference.report_date, actual.report_date) AS data_date,
    toString(coalesce(reference.report_date, actual.report_date)) AS business_key,
    isNotNull(reference.report_date) AS reference_present,
    isNotNull(actual.report_date) AS actual_present,
    toString(tuple(reference.visitors, reference.known_users)) AS reference_value,
    concat(
        toString(tuple(actual.visitors, actual.known_users)),
        ', rows=',
        toString(actual.physical_rows)
    ) AS actual_value,
    isNull(reference.report_date)
        OR isNull(actual.report_date)
        OR actual.physical_rows != 1
        OR NOT isNotDistinctFrom(reference.visitors, actual.visitors)
        OR NOT isNotDistinctFrom(reference.known_users, actual.known_users) AS failed
FROM reference
GLOBAL FULL OUTER JOIN actual USING (report_date)
ORDER BY data_date
SETTINGS join_use_nulls = 1
