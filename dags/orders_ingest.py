"""Приём заказов: пакетный забор слепка из Kafka в хранилище.

Путь Kafka → STG → ODS у заказов один и живёт одним дагом: сначала забор
порции в сырьё, затем разбор того же среза в типизированный ODS и в таблицу
брака. Расписания у дага нет: его дёргает тот, кто положил слепок в топик, —
работник пульта мира после проигрыша дня, — и ждёт конца.

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
import logging

from airflow.sdk import Connection, dag, get_current_context, task

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

    def clickhouse_client():
        # Импорт при выполнении задачи, а не при разборе файла: обработчик DAG
        # разбирает его снова и снова, и импорт наверху оплачивался бы каждым
        # разбором.
        import clickhouse_connect

        connection = Connection.get("clickhouse_default")
        return clickhouse_connect.get_client(
            host=connection.host,
            port=connection.port,
            username=connection.login,
            password=connection.password,
            database=connection.schema or "default",
            connect_timeout=5,
            send_receive_timeout=30,
        )

    # Аргумент с расширением из templates_exts Airflow подменяет текстом файла:
    # берёт его из template_searchpath и прогоняет через Jinja при исполнении
    # задачи, а не при разборе дага. В задачу приезжает готовый запрос — вместе
    # с тем, что файл подключил через include.
    @task(templates_exts=(".sql",))
    def pull_batch(sql: str) -> None:
        load_id = get_current_context()["run_id"]
        client = clickhouse_client()
        try:
            summary = client.command(sql, parameters={"load_id": load_id})
        finally:
            client.close()
        # Размер порции — read_rows: written_rows у вставки в Distributed
        # считает не приехавшее.
        logging.info(
            "порция принята: строк %s, _load_id %s",
            summary.summary["read_rows"],
            load_id,
        )

    @task(templates_exts=(".sql",))
    def parse_batch(good_rows_sql: str, bad_rows_sql: str) -> None:
        """Разобрать срез сырья в версии заказов и в брак.

        Обе вставки в одном task_id: транзакции между ними ClickHouse не даёт,
        а повтор задачи безопасен — срез читается по тому же неизменному
        _load_id (docs/architecture/orders/ingestion.md, «Поток данных»).
        """
        load_id = get_current_context()["run_id"]
        client = clickhouse_client()
        try:
            client.command(good_rows_sql, parameters={"load_id": load_id})
            client.command(bad_rows_sql, parameters={"load_id": load_id})
        finally:
            client.close()
        # Счётчиков строк нет: у запроса с WHERE read_rows считает прочитанное
        # с диска, а не подошедшее (storage.md, «Что проверено»).
        logging.info("срез разобран: _load_id %s", load_id)

    pull_batch("stg/orders_raw_load.sql") >> parse_batch(
        "ods/order_snapshot_load.sql",
        "ods/order_snapshot_errors_load.sql",
    )


orders_ingest()
