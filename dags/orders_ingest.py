"""Приём заказов: пакетный забор слепка из Kafka в хранилище.

Путь Kafka → STG → ODS у заказов один и живёт одним дагом: сначала забор
порции в `stg`, затем разбор того же среза в типизированный ODS и в таблицу
брака. Расписания у дага нет: его дёргает тот, кто положил слепок в топик, —
шаговый даг модельного времени после проигрыша дня, — и ждёт конца.

Контраст с приёмом событий и есть урок. События тянет матвью, навсегда
подписанная на чтеца: приём идёт сам, пока идёт поток. Слепок заказов
приезжает раз в модельный день целой выгрузкой, у которой есть начало и конец,
и забирает её запрос по команде. Push против pull — два режима на одном стенде,
каждый там, где ему место по природе источника.

Решения и доводы целиком — ADR 0008 (забор) и ADR 0010 (версии в ODS); форма
перехода — docs/architecture/orders/ingestion.md.
"""

from __future__ import annotations

import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

# Запросы лежат файлами в sql/ репозитория, разложенные по слоям хранилища;
# сюда их монтирует compose — всем службам Airflow сразу.
SQL_ROOT = "/opt/airflow/sql"


@dag(
    dag_id="orders_ingest",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    # Чтец у топика один, и группа потребителей у него одна. Два прогона разом
    # дрались бы за неё, а слепок разъехался бы по двум _load_id; второй
    # прогон подождёт своей очереди.
    max_active_runs=1,
    template_searchpath=SQL_ROOT,
    tags=["заказы"],
)
def orders_ingest():
    """Забрать приехавший слепок заказов и разложить его по слоям."""
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

    # ClickHouse не даёт транзакции между двумя вставками parse_batch. Один
    # task_id сохраняет их порядок и одну точку повтора для неизменного load_id.
    pull_batch >> parse_batch


orders_ingest()
