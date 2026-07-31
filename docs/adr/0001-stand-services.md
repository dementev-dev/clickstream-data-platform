# ADR 0001. Сервисы каркаса стенда

Дата: 30 июля 2026 года. Статус: принято.

## Решение

Стенд использует Airflow 3.3.0 с LocalExecutor. Airflow разделён на
одноразовый `airflow-init` и три долгоживущих процесса: API, планировщик и
обработчик DAG. Triggerer не запускается: в каркасе нет отложенных задач.
Пример DAG написан через публичный `airflow.sdk`.

Для входа выбран SimpleAuthManager. У него нет команды создания пользователя,
поэтому `airflow-init` записывает пароль администратора из окружения в
JSON-файл отдельного тома `airflow_auth`. Новый именованный том принадлежит
`root`, а Airflow работает от пользователя `airflow`. Поэтому `airflow-init`
запускается от `root`, назначает владельца каталога и сразу выполняет Python и
команды Airflow через `runuser` от пользователя `airflow`. Все файлы
репозитория подключены к этому контейнеру только для чтения. JWT-секрет, ключ
подписи ссылок на журналы и ключ Fernet одинаковы для всех процессов и
приходят из окружения.

Один Postgres 16 хранит две базы метаданных. У Airflow и Superset разные базы,
пользователи и строки подключения. Это экономит один контейнер, но сохраняет
границу владения данными.

Airflow получает подготовленное общее подключение к `clickhouse-01`, а
Superset — подключение `clickhousedb://` к `clickhouse-02`. Провайдер
ClickHouse для Airflow пока не нужен. Разные ноды создают учебную ловушку:
забытый `ON CLUSTER` проявится в Superset, даже если операция Airflow на первой
ноде прошла успешно.

Superset собирается от `apache/superset:6.1.0`. В образ добавлены
`clickhouse-connect` для ClickHouse и `psycopg2-binary` для метаданных
Postgres. Подключение ClickHouse импортируется из YAML при подготовке.

Prometheus читает встроенные точки метрик двух серверов ClickHouse и keeper.
Отдельные сборщики не нужны. Grafana получает источник Prometheus из файла.

## Почему

Разделение Airflow показывает архитектуру третьей версии и даёт честные
проверки здоровья процессов. LocalExecutor достаточен для одного учебного
компьютера. Отказ от triggerer, второго Postgres и внешних сборщиков удерживает
стенд в пределе 3,4 ГБ.

Тома `clickhouse_*_data` хранят данные keeper и двух нод ClickHouse.
`kafka_data`, `postgres_metadata_data`, `superset_home`, `prometheus_data` и
`grafana_data` хранят состояние своих сервисов. `airflow_logs` хранит журналы,
а `airflow_auth` — JSON-файл с учебным паролем администратора. Каталог `dags/`
и все файлы настройки подключены только для чтения, поэтому контейнеры не
меняют рабочее дерево.

## Что проверено

Исследование задачи сверило через Context7 публичный API DAG с Task SDK Airflow
и аргумент `schedule`. Решение дополнительно проверено по документации Airflow
3.3.0: [публичный интерфейс](https://airflow.apache.org/docs/apache-airflow/3.3.0/public-airflow-interface.html),
[архитектура процессов](https://airflow.apache.org/docs/apache-airflow/3.3.0/core-concepts/overview.html),
[SimpleAuthManager](https://airflow.apache.org/docs/apache-airflow/3.3.0/core-concepts/auth-manager/simple/index.html)
и [API здоровья](https://airflow.apache.org/docs/apache-airflow/3.3.0/administration-and-deployment/logging-monitoring/check-health.html).
Оттуда взяты `airflow.sdk`, обязательный отдельный обработчик DAG, возможность
не запускать triggerer и проверка конкретных компонентов здоровья.

Драйверы и строки подключения проверены по документации Superset 6.1.0:
[подключения к базам](https://superset.apache.org/user-docs/6.1.0/databases/),
[ClickHouse](https://superset.apache.org/user-docs/databases/supported/clickhouse/)
и [база метаданных](https://superset.apache.org/admin-docs/6.1.0/configuration/configuring-superset/).
Форма декларативного импорта и команда `test-db` проверены по исходному коду
Superset 6.1.0 и на запущенном образе. Конфигурация метрик проверена по файлам
закреплённого образа ClickHouse 26.3.17.56 и на живых точках серверов и keeper.
