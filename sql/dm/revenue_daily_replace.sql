-- DM: заменяем раздел revenue_daily_rep за выбранный день готовым разделом
-- промежуточной revenue_daily_stage_rep. Раздел таблицы называется партицией.
-- Дату из revenue_daily_scope.sql подставляет Jinja: параметр ClickHouse
-- в ALTER ... PARTITION здесь не поддерживается.
-- ON CLUSTER выполняет замену на локальных таблицах каждого шарда.
-- На одном шарде раздел меняется целиком; общей транзакции для кластера нет.
-- Повтор с теми же строками промежуточной таблицы не добавляет копий в основной.
ALTER TABLE dm.revenue_daily_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.revenue_daily_stage_rep;
