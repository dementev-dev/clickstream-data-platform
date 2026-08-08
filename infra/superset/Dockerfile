FROM apache/superset:6.1.0

USER root

# В основном образе нет ни драйвера ClickHouse, ни драйвера Postgres.
RUN . /app/.venv/bin/activate \
    && uv pip install --no-cache \
        "clickhouse-connect==1.6.0" \
        "psycopg2-binary==2.9.12"

USER superset
