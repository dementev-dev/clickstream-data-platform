-- DM: полный пересчет дневного трафика.
--
-- Публичные _v скрывают Distributed-таблицы от локальной подстановки:
-- EXPLAIN на ClickHouse 26.3 показал чтение обоих шардов на инициаторе.
-- Поэтому GLOBAL назван явно, а физические таблицы не подставлены. LEFT JOIN
-- сохраняет анонимные сессии.
INSERT INTO dm.daily_traffic_dist
(
    report_date,
    visitors,
    known_users,
    _load_id,
    _load_ts
)
SELECT
    sessions.session_date,
    uniqExact(sessions.client_id),
    -- При стендовом join_use_nulls=0 непарный UInt64 получает 0. Условие
    -- не дает анонимной сессии превратиться в известного пользователя.
    uniqExactIf(identities.user_id, identities.user_id != 0),
    {load_id:String},
    now64(3, 'UTC')
FROM dds.session_v AS sessions
GLOBAL LEFT JOIN dds.identity_map_v AS identities USING (client_id)
GROUP BY sessions.session_date
-- Ноль оставляет итоговую группировку на инициаторе: иначе каждый шард записал
-- бы свою строку дня, а FINAL через границу шардов их не объединяет.
SETTINGS distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;
