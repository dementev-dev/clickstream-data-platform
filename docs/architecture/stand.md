# Стенд: службы, доступ, настройка

Здесь описано, из каких служб собран стенд, кто к кому подключается и как
настройки доходят до контейнеров. Состав каркаса обоснован в
[ADR 0001](../adr/0001-stand-services.md), доступ к ClickHouse — в
[ADR 0007](../adr/0007-clickhouse-access.md). Адреса и пароли интерфейсов
остаются в [корневом README](../../README.md#состав-и-доступ).

## Службы

- `clickhouse-01` — инициатор DDL и точка подключения Airflow;
- `clickhouse-02` — точка подключения Superset;
- `clickhouse-keeper` — координатор кластера;
- `kafka` — один брокер KRaft;
- `kafka-exporter` — экспортёр метрик Kafka для Prometheus;
- `postgres-metadata` — один Postgres с отдельными базами и пользователями
  `airflow` и `superset`;
- `airflow-apiserver`, `airflow-scheduler` и `airflow-dag-processor` —
  Airflow 3.3 с LocalExecutor, без triggerer;
- `superset` — интерфейс с заранее настроенным подключением к ClickHouse;
- `prometheus` и `grafana` — сбор и просмотр метрик ClickHouse и Kafka.

## Пользователи и роли ClickHouse

В ClickHouse семь пользователей и шесть ролей:

- `etl` — учётная запись рабочих дагов Airflow; роль `etl_writer` читает и
  пишет слои хранилища;
- `lifecycle` — учётная запись дагов жизненного цикла; роль `lifecycle_owner`
  разрешает им применять канонический DDL и пересоздавать прикладные базы. У
  рабочих дагов этих административных прав нет;
- `bi` — учётная запись Superset; роль `bi_reader` читает ODS, DDS, DM и
  справочники `dic`, а запросы Superset обращаются к публичным `_v`;
- `analyst` — учётная запись аналитика с правом чтения всех слоёв и
  справочников;
- `grafana` — учётная запись Grafana; роль `monitoring_reader` читает все слои,
  системные таблицы и данные со всех реплик кластера;
- `dict` — беспарольная учётная запись словаря товаров с правом чтения базы
  `dic`; словарь читает через неё свою подложку;
- `default` — служебная учётная запись для проверок состояния и скриптов внутри
  контейнеров; приложения с ней не подключаются.

Почему пользователи объявлены файлом, а ноды доверяют общему секрету, — в
[ADR 0007](../adr/0007-clickhouse-access.md).
Пользователи и роли объявлены в `infra/clickhouse/users.d/access.xml`. Пароли и
общий секрет нод живут в `.env` и передаются в конфигурацию через окружение;
Superset получает пароль `bi`, а Grafana — пароль `grafana` тем же путём.
Значения для локального стенда есть в `.env.example`.

## Применение настроек

Чтобы применить новые значения ClickHouse из `.env` или изменения файлов в
`infra/clickhouse/config.d/` и `infra/clickhouse/users.d/`, пересоздайте ноды:

```bash
docker compose up --force-recreate --wait clickhouse-01 clickhouse-02
```

Команда возвращает управление, когда обе ноды снова здоровы; именованные тома
при этом сохраняются. `docker compose restart` оставит прежнее окружение и
может оставить старую версию отдельно смонтированного файла.

## Секреты и порты

В локальном учебном стенде намеренно используются простые ненастоящие пароли и
ключи. Kafka и Prometheus работают без проверки доступа. Это не пример настройки
защиты: не копируйте значения из `.env.example` в рабочую среду. Все
опубликованные порты привязаны только к `127.0.0.1`; Postgres наружу не
опубликован.

## Одна реплика на шард

В бою перед репликами ClickHouse обычно был бы балансировщик. Здесь в каждом
шарде одна реплика, поэтому балансировать нечего. Балансировщик и топология
2×2 намеренно не входят в стенд.

## Что проверено

**Конфигурация сверена 30 июля 2026 года** с официальной документацией
ClickHouse:
[настройками сервера](https://clickhouse.com/docs/operations/server-configuration-parameters/settings),
[Keeper](https://clickhouse.com/docs/guides/oss/deployment-and-scaling/keeper/),
[ReplicatedMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication),
[ON CLUSTER](https://clickhouse.com/docs/sql-reference/distributed-ddl) и
[Distributed](https://clickhouse.com/docs/engines/table-engines/special/distributed).
Кластер описан в `remote_servers`, макросы — в `macros`, а подключение к
keeper — в `zookeeper`. Путь `ReplicatedMergeTree` содержит `{shard}` и
`{replica}`.
Для `Distributed` задаются имя кластера, база, локальная таблица и ключ
шардирования. Макросы выбраны, чтобы один DDL через `ON CLUSTER` создавал
отдельный путь для каждого шарда без вписанных вручную значений. В образе
закреплена точная версия текущего LTS-выпуска —
[26.3.17.56](https://github.com/ClickHouse/ClickHouse/releases/tag/v26.3.17.56-lts);
серверы и keeper используют один образ.

Настройки Kafka 4.3.1 сверены с
[примером односерверного KRaft](https://github.com/apache/kafka/blob/4.3.1/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml).

Раздел метрик взят из конфигурации закреплённого образа ClickHouse и проверен
на серверах и keeper. Автоматическая настройка источников данных Grafana
сверена с
[официальным описанием автоматической настройки](https://grafana.com/docs/grafana/latest/administration/provisioning/)
и [документацией плагина ClickHouse](https://grafana.com/docs/plugins/grafana-clickhouse-datasource/latest/configure/).
Prometheus собирает встроенные метрики двух серверов и keeper, а через
`kafka-exporter` — состояние Kafka. Поверх этого в Grafana поднимается дашборд
«Данные»: свежесть, поток, брак, сходимость Kafka с ClickHouse, отставание
чтения и дневной слепок заказов. Устройство зоны — в [справочнике
мониторинга](monitoring/README.md), разбор панелей — в
[описании дашборда](monitoring/data.md); правил оповещения
пока нет.

Airflow закреплён на 3.3.0. Состав обязательных процессов, LocalExecutor,
публичный `airflow.sdk`, API проверки состояния и SimpleAuthManager сверены с
[архитектурой Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/3.3.0/core-concepts/overview.html),
[публичным интерфейсом](https://airflow.apache.org/docs/apache-airflow/3.3.0/public-airflow-interface.html)
и [описанием здоровья](https://airflow.apache.org/docs/apache-airflow/3.3.0/administration-and-deployment/logging-monitoring/check-health.html).
Для ClickHouse установлен официальный провайдер Airflow: SQL-файлы исполняет
общий `SQLExecuteQueryOperator`, программные запросы идут через
`ClickHouseHook`. Провайдер работает поверх закреплённого
`clickhouse-connect`. Официальный провайдер Kafka уже использует
`confluent-kafka`. В стенде клиент добавлен в образ напрямую, без отдельного
подключения Airflow и обёрток провайдера.

Superset закреплён на 6.1.0; драйвер `clickhouse-connect`, схема адреса
подключения `clickhousedb://` и драйвер Postgres сверены с
[документацией подключений Superset](https://superset.apache.org/user-docs/6.1.0/databases/)
и [настройкой базы метаданных](https://superset.apache.org/admin-docs/6.1.0/configuration/configuring-superset/).
