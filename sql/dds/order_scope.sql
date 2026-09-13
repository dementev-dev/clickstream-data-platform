-- Дни пересборки заказов: first_day и last_day, обе границы включены.
-- Текущее окно — дни оформления заказов из последнего snapshot_date.
-- Если в DDS нет дня, который есть в ODS, отрезок расширяется до первого
-- такого дня. Это восстанавливает пропуски даже за пределами текущего окна.
--
-- Дата слепка у актуальной версии показывает последнее наблюдение заказа
-- (ADR 0014). Поэтому окно берется из данных, без копирования константы
-- генератора. FINAL здесь не нужен: created_at одинаков у всех версий,
-- а строки последнего слепка сохраняются при слияниях.
--
-- Пустой ODS не дает строки результата; даг завершится ошибкой.
-- Точка с запятой в конце не нужна: клиент дописывает FORMAT ответа.
-- Порядок пересборки — docs/architecture/etl.md, «Объём прогона».

WITH
    (SELECT max(snapshot_date) FROM ods.order_dist) AS last_snapshot,
    (
        SELECT min(toDate(toTimeZone(created_at, 'Europe/Samara')))
        FROM ods.order_dist
        WHERE snapshot_date = last_snapshot
    ) AS window_start,
    (
        SELECT max(toDate(toTimeZone(created_at, 'Europe/Samara')))
        FROM ods.order_dist
        WHERE snapshot_date = last_snapshot
    ) AS window_end,
    -- GLOBAL NOT IN сравнивает дни со всем DDS. Локальное сравнение могло бы
    -- счесть день пропущенным, если его заказы находятся на соседнем шарде.
    -- minOrNull возвращает NULL, когда пропущенных дней нет.
    (
        SELECT minOrNull(day)
        FROM (
            SELECT DISTINCT toDate(toTimeZone(created_at, 'Europe/Samara')) AS day
            FROM ods.order_dist
        )
        WHERE day GLOBAL NOT IN (SELECT order_date FROM dds.order_dist)
    ) AS first_missing_day
SELECT
    -- Начало окна или первый пропущенный день, если он раньше.
    least(window_start, coalesce(first_missing_day, window_start)) AS first_day,
    window_end AS last_day
WHERE (SELECT count() FROM ods.order_dist) > 0
