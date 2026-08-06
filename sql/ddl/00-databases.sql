-- Базы слоёв хранилища.
--
-- Файлы этой папки применяются по порядку имён с ноды 1 и всегда ON CLUSTER:
-- объекты обязаны появиться на обеих нодах, иначе распределённая таблица
-- окажется лицом половины кластера. Имя кластера — clickstream_cluster,
-- задано в infra/clickhouse/config.d/cluster.xml.
--
-- Идемпотентность везде через IF NOT EXISTS: make up применяет эти файлы и
-- поверх живого тома.

CREATE DATABASE IF NOT EXISTS stg ON CLUSTER clickstream_cluster;

-- База ODS заводится здесь же, хотя её объекты приносит #43: базы дёшевы, а
-- порядок файлов от этого не зависит.
CREATE DATABASE IF NOT EXISTS ods ON CLUSTER clickstream_cluster;
