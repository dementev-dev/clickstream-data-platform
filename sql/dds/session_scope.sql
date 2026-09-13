-- DDS: дни для сборки сессий, от first_day до last_day включительно.
-- Пустой ответ означает пустой ODS; NULL в first_day — отсутствие работы.
--
-- position — номер следующего дня для генерации. Последний прожитый день
-- равен min(EventDate) + position - 1. В отличие от max(EventDate), эта
-- граница исключает еще идущий день. Ранние события ODS не удаляются по TTL,
-- поэтому min(EventDate) сохраняет начало модельного календаря.
--
-- До этого запроса даг ждет чтения hits и опустошения очереди ods.event_dist.
-- Позиция мира сама по себе не подтверждает доставку событий.
--
-- Точки с запятой в конце нет: драйвер добавляет FORMAT к этому запросу.
WITH
    (SELECT min(EventDate) FROM ods.event_dist) AS first_source_day,
    first_source_day + ({position:UInt16} - 1) AS last_lived_day,
    -- Ищем первый день ODS, которого нет в DDS: максимум собранного дня
    -- пропустил бы более ранний пробел. Частично заполненный день так не найти.
    -- GLOBAL сравнивает со списком дней всего DDS, а не одного шарда.
    -- minOrNull возвращает NULL, если пропусков нет.
    (
        SELECT minOrNull(day)
        FROM (SELECT DISTINCT EventDate AS day FROM ods.event_dist)
        WHERE day GLOBAL NOT IN (SELECT session_date FROM dds.session_dist)
    ) AS first_missing_day
SELECT
    -- Отсутствующий день попадет в сборку только после его завершения.
    if(first_missing_day <= last_lived_day, first_missing_day, NULL) AS first_day,
    last_lived_day AS last_day
-- FINAL не нужен: повтор доставки не меняет начало календаря и набор дат.
WHERE (SELECT count() FROM ods.event_dist) > 0
