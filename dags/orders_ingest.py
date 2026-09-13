"""Пакетный прием заказов: Kafka → STG → ODS.

Шаговый даг модельного времени отправляет слепок, запускает orders_ingest
и ждет его завершения. Собственного расписания у orders_ingest нет.
Сначала pull_batch сохраняет порцию сообщений в STG, затем parse_batch
разбирает тот же срез по _load_id в заказы и ошибки.

События принимаются непрерывно через материализованные представления.
Заказы читаются одним запросом; полнота слепка этим не гарантируется.
Правила и ограничения — docs/architecture/orders/ingestion.md.
"""

from __future__ import annotations

import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

# Compose монтирует sql/ по этому пути во все службы Airflow.
SQL_ROOT = "/opt/airflow/sql"


@dag(
    dag_id="orders_ingest",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    # Один запуск за раз: параллельные чтения разделили бы порцию
    # между разными _load_id.
    max_active_runs=1,
    template_searchpath=SQL_ROOT,
    tags=["заказы"],
)
def orders_ingest():
    """Сохранить порцию заказов и разобрать ее в ODS."""
    pull_batch = SQLExecuteQueryOperator(
        task_id="pull_batch",
        conn_id="clickhouse_default",
        sql="stg/orders_raw_load.sql",
        parameters={"load_id": "{{ run_id }}"},
        do_xcom_push=False,
    )
    parse_batch = SQLExecuteQueryOperator(
        task_id="parse_batch",
        conn_id="clickhouse_default",
        sql="ods/order_parse.sql",
        parameters={"load_id": "{{ run_id }}"},
        split_statements=True,
        do_xcom_push=False,
    )

    # Две вставки parse_batch не образуют транзакцию. Повтор задачи
    # выполняет обе над тем же срезом STG по _load_id.
    pull_batch >> parse_batch


orders_ingest()
