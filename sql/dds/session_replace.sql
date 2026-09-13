-- DDS: замена одного дня в session_rep готовой партицией session_stage_rep.
-- ON CLUSTER выполняет замену на локальных таблицах каждого шарда.
-- Замена атомарна на одном шарде, общей транзакции для кластера нет.
-- Повтор с тем же донором не добавляет копий строк.
--
-- Дату из session_scope.sql подставляет Jinja: параметр ClickHouse в
-- ALTER ... PARTITION здесь не поддерживается. Поиск дней следующего
-- запуска не обнаружит неполный день, если часть его строк уже есть в DDS.
ALTER TABLE dds.session_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dds.session_stage_rep;
