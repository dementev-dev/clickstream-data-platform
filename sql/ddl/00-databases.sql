-- Базы хранилища: слои цепочки и зона справочников.
--
-- Файлы этой папки применяются по порядку имён с ноды 1 и всегда ON CLUSTER:
-- объекты обязаны появиться на обеих нодах, иначе распределённая таблица
-- окажется лицом половины кластера. Имя кластера — clickstream_cluster,
-- задано в infra/clickhouse/config.d/cluster.xml.
--
-- Идемпотентность везде через IF NOT EXISTS: world_initialize может повторить
-- применение после оборванного первого запуска.

CREATE DATABASE IF NOT EXISTS stg ON CLUSTER clickstream_cluster;

-- База ODS заводится здесь же, хотя её объекты приносит #43: базы дёшевы, а
-- порядок файлов от этого не зависит.
CREATE DATABASE IF NOT EXISTS ods ON CLUSTER clickstream_cluster;

-- База DDS заводится здесь же по той же причине, что и ODS.
CREATE DATABASE IF NOT EXISTS dds ON CLUSTER clickstream_cluster;

-- База витрин завершает цепочку STG → ODS → DDS → DM.
CREATE DATABASE IF NOT EXISTS dm ON CLUSTER clickstream_cluster;

-- Справочники стоят вне цепочки STG → ODS → DDS → DM: в хранилище их никто не
-- производит, а читают их несколько слоёв — ADR 0012. Порядок слоёв к зоне не
-- применяется, её файл идёт сразу за этим.
CREATE DATABASE IF NOT EXISTS dic ON CLUSTER clickstream_cluster;
