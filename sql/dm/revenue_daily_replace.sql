-- DM: замена дня выручки готовой партицией донора.
-- Дату из revenue_daily_scope.sql подставляет Jinja: параметр ClickHouse
-- в ALTER ... PARTITION здесь не поддерживается.
-- ON CLUSTER выполняет замену на локальных таблицах каждого шарда.
-- Замена атомарна на одном шарде, общей транзакции для кластера нет.
-- Повтор с тем же донором не добавляет копий строк.
ALTER TABLE dm.revenue_daily_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.revenue_daily_stage_rep;
