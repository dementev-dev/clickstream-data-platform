FROM apache/airflow:3.3.0

USER root

# SQL-файлы ClickHouse исполняет официальный провайдер Airflow. Он разрешает
# любую версию clickhouse-connect >= 1.3.0, поэтому драйвер закреплён отдельно:
# сборка стенда не должна незаметно менять транспорт при обновлении зависимостей.
#
# Официальный провайдер Kafka использует тот же confluent-kafka. Пробникам
# не нужны его подключения и обёртки, поэтому в образ добавлен сам клиент.
#
# Провайдера docker, которым пульт мира зовёт генератор, ставить не нужно:
# база несёт его сама — 4.5.7 к Airflow 3.3.0, замер 13 августа 2026 года.
RUN uv pip install --python /home/airflow/.local/bin/python --no-cache \
        "apache-airflow-providers-clickhousedb==1.0.0" \
        "clickhouse-connect==1.6.0" \
        "confluent-kafka==2.15.0" \
    && python -c "import airflow.providers.clickhousedb, clickhouse_connect, confluent_kafka"

USER airflow
