ARG AIRFLOW_BASE_IMAGE
FROM ${AIRFLOW_BASE_IMAGE}

USER root

# Официальный провайдер Kafka использует тот же confluent-kafka. Пробникам
# не нужны его подключения и обёртки, поэтому в образ добавлен сам клиент.
RUN uv pip install --python /home/airflow/.local/bin/python --no-cache \
        "clickhouse-connect==1.6.0" \
        "confluent-kafka==2.15.0" \
    && python -c "import clickhouse_connect, confluent_kafka"

USER airflow
