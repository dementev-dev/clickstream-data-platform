# ADR 0001. Сервисы каркаса стенда

Дата: 30 июля 2026 года. Статус: принято; ресурсный довод отозван
[ADR 0004](0004-resource-limits.md).

## Решение

Стенд использует Airflow 3.3.0 с LocalExecutor. Airflow разделён на
одноразовый `airflow-init` и три долгоживущих процесса: API, планировщик и
обработчик DAG. Triggerer не запускается: в каркасе нет отложенных задач.
Два интеграционных пробника написаны через публичный `airflow.sdk`.

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

Airflow получает два подготовленных подключения к `clickhouse-01`: рабочее
`clickhouse_default` и административное `clickhouse_lifecycle` для дагов
жизненного цикла мира ([ADR 0013](0013-world-lifecycle.md)). Superset получает
подключение `clickhousedb://` к `clickhouse-02`. Провайдер ClickHouse для
Airflow не нужен: локальный образ содержит прямой клиент.
Пробник создаёт служебные таблицы `ON CLUSTER`, пишет на первой ноде и через
`remote` читает `Distributed` на второй. Разные ноды сохраняют учебную
ловушку: забытый `ON CLUSTER` проявится в Superset, даже если операция Airflow
на первой ноде прошла успешно.

Клиент Kafka также добавлен прямо в образ Airflow. Официальный провайдер
использует тот же `confluent-kafka`, а пробнику не нужны его подключение,
операторы и обёртки. Топик пробника постоянный, сообщение каждого запуска
отличается уникальным маркером.

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

Ресурсный довод предыдущего абзаца отозван
[ADR 0004](0004-resource-limits.md): предела 3,4 ГБ у стенда нет, вместо него
объявлено требование к машине. Сами решения остаются в силе по остальным
основаниям, названным выше. Отказ от внешних сборщиков вдобавок частично
пересмотрен [ADR 0002](0002-monitoring-scope.md): сборщик метрик Kafka нужен
ради отставания чтения.

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

Для интеграционных пробников проверены `Connection.get` в публичном
`airflow.sdk`, запросы через `clickhouse-connect` и состав официального
провайдера Kafka. Выбран прямой `confluent-kafka`: провайдер строит свои
подключения и обёртки поверх него, которые двум коротким пробникам не нужны.

Драйверы и строки подключения проверены по документации Superset 6.1.0:
[подключения к базам](https://superset.apache.org/user-docs/6.1.0/databases/),
[ClickHouse](https://superset.apache.org/user-docs/databases/supported/clickhouse/)
и [база метаданных](https://superset.apache.org/admin-docs/6.1.0/configuration/configuring-superset/).
Форма декларативного импорта и команда `test-db` проверены по исходному коду
Superset 6.1.0 и на запущенном образе. Конфигурация метрик проверена по файлам
закреплённого образа ClickHouse 26.3.17.56 и на живых точках серверов и keeper.
