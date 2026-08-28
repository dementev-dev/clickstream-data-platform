-- DQ: область посуточной сверки обогащенных событий.
--
-- Объединение сторон оставляет видимым день, потерянный целиком.

SELECT day
FROM
(
    SELECT event_date AS day
    FROM dds.event_v
    UNION ALL
    SELECT event_date AS day
    FROM dm.events_enriched_v
)
GROUP BY day
ORDER BY day
