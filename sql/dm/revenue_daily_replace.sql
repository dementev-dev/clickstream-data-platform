-- DM: подмена одного дня выручки готовой партицией донора.
--
-- День приходит из результата revenue_daily_scope.sql, а не от пользователя.
-- Jinja нужна потому, что ClickHouse не подставляет параметр в PARTITION.
-- Замена идет на локальной таблице ON CLUSTER: граница атомарности и повтор
-- разобраны на таком же шаге в sql/dds/order_replace.sql.
ALTER TABLE dm.revenue_daily_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.revenue_daily_stage_rep;
