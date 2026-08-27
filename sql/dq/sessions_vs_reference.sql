-- DQ: эталонная сессия Метрики против сессии, нарезанной слоем DDS.
--
-- Строка результата — одна кука одного завершённого дня. Сравнение по ключу
-- не даёт лишней сессии одной куки скрыть потерянную сессию другой.
WITH
    (SELECT min(EventDate) FROM ods.event_v) AS first_source_day,
    first_source_day + ({position:UInt16} - 1) AS last_lived_day,
    reference AS
    (
        SELECT
            EventDate AS data_date,
            ClientID AS client_id,
            uniqExact(VisitID) AS sessions
        FROM ods.event_v
        WHERE EventDate <= last_lived_day
        GROUP BY
            data_date,
            client_id
    ),
    actual AS
    (
        SELECT
            session_date AS data_date,
            client_id,
            count() AS sessions
        FROM dds.session_v
        WHERE session_date <= last_lived_day
        GROUP BY
            data_date,
            client_id
    )
SELECT
    coalesce(reference.data_date, actual.data_date) AS data_date,
    toString(coalesce(reference.client_id, actual.client_id)) AS business_key,
    isNotNull(reference.client_id) AS reference_present,
    isNotNull(actual.client_id) AS actual_present,
    toString(reference.sessions) AS reference_value,
    toString(actual.sessions) AS actual_value,
    NOT isNotDistinctFrom(reference.sessions, actual.sessions) AS failed
FROM reference
GLOBAL FULL OUTER JOIN actual USING (data_date, client_id)
ORDER BY
    data_date,
    business_key
SETTINGS join_use_nulls = 1
