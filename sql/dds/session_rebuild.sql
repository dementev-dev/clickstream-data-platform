-- DDS: расчет сессий за дни от first_day до last_day включительно.
-- Сначала очищаем промежуточную таблицу session_stage_rep, затем записываем
-- в нее рассчитанные строки через session_stage_dist. Основная session_rep
-- остается прежней до выполнения session_replace.sql.
--
-- Первое окно помечает событие единицей, если с предыдущего события того же
-- посетителя за тот же день прошло больше 1800 секунд. Второе окно суммирует
-- эти единицы: получается номер визита для каждого посетителя за день.
-- PARTITION BY (client_id, event_date) считает каждый модельный день отдельно:
-- события разных дней не попадут в одну сессию даже при короткой паузе.
-- Учитываются все типы событий.
--
-- У первого события lagInFrame возвращает его собственное время: разница
-- равна нулю, opens_visit = 0, и первый visit_no тоже равен нулю.
-- Оба окна используют (event_time, event_id), чтобы при равном времени
-- отметка начала и номер визита относились к одним событиям.
-- Порог 1800 секунд — правило визита; он не передается параметром.
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
    -- Идентификатор зависит от посетителя и начала визита.
    -- Повторная сборка тех же событий дает тот же session_id.
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
    -- UTM, устройство и город берутся по минимальному event_time.
    -- Как и у страниц выше, при равном времени выбор между событиями
    -- не определен: event_id в эти argMin/argMax не входит.
    -- Устройство и город на стенде постоянны для одной куки.
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
-- Источник и цель распределены по cityHash64(client_id): локальная вставка
-- сохраняет нужную раскладку, parallel_distributed_insert_select менять
-- не требуется. Перед заменой партиций ждем доставки строк на шарды.
SETTINGS distributed_foreground_insert = 1;
