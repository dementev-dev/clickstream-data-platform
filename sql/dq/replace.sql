-- DQ: публикация одного полностью собранного дня общей сводки.
--
-- День приходит из самого донора после успеха всех пяти сборок. REPLACE
-- PARTITION работает с локальными таблицами одинаковой структуры; это
-- ограничение сверено по документации ClickHouse 27 августа 2026 года.
ALTER TABLE dm.dq_summary_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.dq_summary_stage_rep;
