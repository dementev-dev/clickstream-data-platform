-- DDS: заменяем раздел session_rep за выбранный день готовым разделом
-- промежуточной session_stage_rep. Раздел таблицы называется партицией.
-- ON CLUSTER выполняет замену на локальных таблицах каждого шарда.
-- На одном шарде раздел меняется целиком; общей транзакции для кластера нет.
-- Повтор с теми же строками session_stage_rep не добавляет копий в session_rep.
--
-- Дату из session_scope.sql подставляет Jinja: параметр ClickHouse в
-- ALTER ... PARTITION здесь не поддерживается. Поиск дней следующего
-- запуска не обнаружит неполный день, если часть его строк уже есть в DDS.
ALTER TABLE dds.session_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dds.session_stage_rep;
