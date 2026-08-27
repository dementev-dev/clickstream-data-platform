-- DQ: общий донор очищается перед сборкой всех проверок.
--
-- Если одна сборка упадет, публикация не начнется. Следующий запуск удалит
-- неполный результат здесь и соберет донор заново.

ALTER TABLE dm.dq_summary_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;
