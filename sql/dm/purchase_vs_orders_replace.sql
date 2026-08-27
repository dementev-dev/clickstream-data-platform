-- DM: подмена одного дня сверки собранной партицией донора.
--
-- День для PARTITION берет purchase_vs_orders_scope.sql. Выбор Jinja,
-- атомарность и повтор разобраны в sql/dds/order_replace.sql.
--
-- Пустая партиция законна: если источник больше не показывает строк дня,
-- замена убирает этот день из витрины.
ALTER TABLE dm.purchase_vs_orders_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.purchase_vs_orders_stage_rep;
