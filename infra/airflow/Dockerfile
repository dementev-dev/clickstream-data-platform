FROM apache/airflow:3.3.0

USER root

# Официальный провайдер Kafka использует тот же confluent-kafka. Пробникам
# не нужны его подключения и обёртки, поэтому в образ добавлен сам клиент.
#
# Провайдера docker, которым пульт мира зовёт генератор, ставить не нужно:
# база несёт его сама — 4.5.7 к Airflow 3.3.0, замер 13 августа 2026 года.
RUN uv pip install --python /home/airflow/.local/bin/python --no-cache \
        "clickhouse-connect==1.6.0" \
        "confluent-kafka==2.15.0" \
    && python -c "import clickhouse_connect, confluent_kafka"

USER airflow
