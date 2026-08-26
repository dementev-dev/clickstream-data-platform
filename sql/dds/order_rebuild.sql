-- DDS: сборка дней заказа в таблицу-двойник перед заменой партиций.
--
-- Двойник очищается целиком в начале сборки, а не в конце: собранное остаётся
-- лежать до следующего запуска, и разбор происшествия видит ровно то, чем
-- подменили день. Очистка обязана быть первой ещё и по делу — вставка поверх
-- прежних строк удвоила бы день в доноре, а REPLACE PARTITION перенёс бы
-- удвоение в цель.
--
-- Дни собираются одной вставкой, а не по дню за раз: источник читается один
-- раз на весь отрезок. Кто раскладывает строки по шардам — вставка через
-- Distributed, ключ cityHash64(order_id) — разобрано в DDL двойника.
--
-- Чистит двойник DROP PARTITION ALL, а не TRUNCATE, и это не вкусовщина:
-- TRUNCATE — отдельное право, роли etl оно не выдано, а снятие всех партиций
-- входит в ALTER, который у неё есть (ADR 0007). Замерено: TRUNCATE отвечает
-- ролью etl кодом 497, DROP PARTITION ALL проходит. Расширять права ради
-- операции, для которой уже есть разрешённая форма, значило бы платить
-- доступом за синоним.

ALTER TABLE dds.order_stage_rep ON CLUSTER clickstream_cluster DROP PARTITION ALL;

INSERT INTO dds.order_stage_dist
(
    order_id,
    user_id,
    status,
    order_date,
    created_at,
    updated_at,
    items_total,
    discount,
    delivery,
    total,
    item_sku,
    item_quantity,
    item_price,
    _load_id,
    _load_ts
)
-- Позиции разбираются здесь один раз, на границе ODS → DDS. Цена на проводе —
-- строка с двумя знаками, и JSONExtract читает её прямо в Decimal: замерено на
-- живом слепке, промежуточный toDecimal64 не нужен.
--
-- Разбор по типу молчалив, и знать об этом надо заранее. Строгий приём ODS
-- проверил только, что items — массив, внутрь он не смотрел; здесь тоже никто
-- не смотрит. Замерено: qty вида "oops" даёт 0, пропущенная цена даёт 0,
-- скаляр вместо объекта выпадает из массива целиком. На каноническом слепке
-- генератора такого не бывает, и сумма «цена × количество» сходится с
-- items_total у всех заказов, но сходится она по свойству источника, а не
-- потому, что её здесь кто-то проверяет. Постоянное место такой проверки —
-- контур DQ этапа 4; пока она делается руками при приёмке.
WITH JSONExtract(
        items, 'Array(Tuple(sku String, qty UInt32, price Decimal(18, 2)))'
    ) AS lines
SELECT
    order_id,
    user_id,
    status,
    -- День заказа и оба времени переезжают в пояс счётчика: DDS обслуживает
    -- человека с дашбордом, и пояс называется здесь в последний раз.
    toDate(toTimeZone(created_at, 'Europe/Samara')),
    toTimeZone(created_at, 'Europe/Samara'),
    toTimeZone(updated_at, 'Europe/Samara'),
    items_total,
    discount,
    delivery,
    total,
    arrayMap(line -> line.sku, lines),
    arrayMap(line -> line.qty, lines),
    arrayMap(line -> line.price, lines),
    -- Координаты запуска, собравшего день: у всех строк дня они одни.
    {load_id:String},
    now64(3, 'UTC')
FROM ods.order_v
WHERE toDate(toTimeZone(created_at, 'Europe/Samara'))
        BETWEEN {first_day:Date} AND {last_day:Date}
    -- Вторая граница не меняет отбор, а открывает источнику отсечение
    -- партиций: ODS нарезан по toDate(created_at) в UTC, и условие на
    -- вычисленный день заказа для этого ключа непрозрачно — сервер прочёл бы
    -- все партиции. Сутки пояса счётчика ложатся ровно на две даты UTC,
    -- поэтому день слева берётся с запасом.
    AND toDate(created_at) BETWEEN {first_day:Date} - 1 AND {last_day:Date}
-- Тот же ноль, что в приёме заказов: заводская двойка исполнила бы вставку
-- локально на каждом шарде, минуя ключ шардирования цели, и строки унаследовали
-- бы раскладку источника. Ключи ODS и DDS здесь совпадают, и раскладка вышла бы
-- верной — но по совпадению, а не по контракту (docs/architecture/storage.md,
-- «Раскладка по шардам», капкан #137).
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
