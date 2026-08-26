-- DDS: нарезка событий на визиты и сборка дней в таблицу-двойник.
--
-- Двойник очищается целиком в начале, дни собираются одной вставкой на весь
-- отрезок — по правилу заказа (sql/dds/order_rebuild.sql).
--
-- Нарезка — центральный расчёт слоя, и стоит она на двух окнах подряд.
-- Первое спрашивает у каждого события, сколько прошло с предыдущего события
-- той же куки в тот же день, и ставит единицу там, где пауза длиннее
-- таймаута: это шов между визитами. Второе считает нарастающий итог этих
-- единиц — номер визита внутри дня куки. Дальше визит становится обычной
-- группировкой.
--
-- Границу суток окна не проверяют: она уже в разбиении PARTITION BY
-- (client_id, event_date), поэтому визит через полночь не склеится и при
-- короткой паузе. Так правило визита ложится в запрос целиком: одна кука,
-- один день, паузы не длиннее таймаута (CONTEXT.md, «Визит»).
--
-- Первое событие куки за день сравнивается само с собой — третий аргумент
-- lagInFrame. Паузы у него нет, шов ему не нужен, и номер первого визита
-- получается нулём: номера различают визиты внутри дня куки, а не считают их
-- от единицы.
--
-- Порядок в окнах полный — метка времени и event_id. Окна считаются двумя
-- разными сортировками (видно в EXPLAIN PIPELINE), и при равных метках они
-- вправе разойтись: шов достался бы одной строке, а нарастающий итог другой,
-- и сессия разъехалась бы надвое.
--
-- Тридцать минут написаны числом, а не приехали параметром: это правило
-- аналитики, а не константа мира. Генератор режет визиты тем же порогом;
-- разойдись эти два числа — увидит счётная сверка с VisitID
-- (docs/architecture/dds/session.md, «Сверка с эталоном»).

ALTER TABLE dds.session_stage_rep ON CLUSTER clickstream_cluster DROP PARTITION ALL;

INSERT INTO dds.session_stage_dist
(
    session_id,
    client_id,
    session_date,
    started_at,
    finished_at,
    duration_seconds,
    events_total,
    pageviews,
    cart_adds,
    purchases,
    entry_url,
    exit_url,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    device_category,
    region_city,
    _load_id,
    _load_ts
)
WITH
    seams AS (
        SELECT
            client_id,
            event_date,
            event_time,
            event_id,
            event_type,
            url,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term,
            device_category,
            region_city,
            event_time - lagInFrame(event_time, 1, event_time) OVER visit > 1800
                AS opens_visit
        FROM dds.event_v
        WHERE event_date BETWEEN {first_day:Date} AND {last_day:Date}
        WINDOW visit AS (
            PARTITION BY client_id, event_date
            ORDER BY event_time, event_id
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        )
    ),
    numbered AS (
        SELECT
            *,
            sum(opens_visit) OVER (
                PARTITION BY client_id, event_date
                ORDER BY event_time, event_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS visit_no
        FROM seams
    )
SELECT
    -- Чекан визита: кука и его начало. Время входит в хеш секундой эпохи,
    -- поэтому id держится за содержание сессии, а не за порядок сборки.
    cityHash64(client_id, min(event_time)),
    client_id,
    event_date,
    min(event_time),
    max(event_time),
    max(event_time) - min(event_time),
    count(),
    countIf(event_type = 'pageview'),
    countIf(event_type = 'add_to_cart'),
    countIf(event_type = 'purchase'),
    argMin(url, event_time),
    argMax(url, event_time),
    -- Атрибуция и паспорт куки берутся с первого события визита. У паспорта
    -- любое событие дало бы то же: устройство и город у куки постоянны.
    argMin(utm_source, event_time),
    argMin(utm_medium, event_time),
    argMin(utm_campaign, event_time),
    argMin(utm_content, event_time),
    argMin(utm_term, event_time),
    argMin(device_category, event_time),
    argMin(region_city, event_time),
    {load_id:String},
    now64(3, 'UTC')
FROM numbered
GROUP BY client_id, event_date, visit_no
-- Капкана parallel_distributed_insert_select здесь нет, и это выигрыш
-- ко-локации: ключ шардирования у источника и цели один — cityHash64 от
-- куки, — поэтому визит ляжет туда же, где лежали его события, исполнись
-- вставка хоть на инициаторе, хоть локально на шардах. Заказу приходится
-- называть ноль вслух: совпадение ключей у него случайное
-- (sql/dds/order_rebuild.sql). Здесь выбор сделан обратный и он тоже
-- осознанный: настройку не называть, чтобы не притворяться, будто её значение
-- что-то решает.
SETTINGS distributed_foreground_insert = 1;
