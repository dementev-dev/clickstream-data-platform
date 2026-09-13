"""Ручная самопроверка учебных витрин DM против их источников."""

from __future__ import annotations

import datetime
import logging
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.sdk import dag, task

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
SQL_ROOT = Path("/opt/airflow/sql/dq")
CLICKHOUSE_CONNECTION = "clickhouse_default"
DETAIL_LIMIT = 20


def _clickhouse_hook() -> ClickHouseHook:
    """Подключение к ClickHouse для чтения обеих сторон сверки."""
    return ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)


def _mart_exists(hook: ClickHouseHook, mart: str) -> bool:
    """Проверить наличие публичной витрины на ноде подключения."""
    database, name = mart.split(".", maxsplit=1)
    rows = hook.get_records(
        """
        SELECT count()
        FROM system.tables
        WHERE database = {database:String} AND name = {name:String}
        """,
        parameters={"database": database, "name": name},
    )
    return rows[0][0] == 1


@task
def check_mart(
    mart: str,
    lab_path: str,
    sql_name: str,
    scope_sql_name: str | None = None,
) -> None:
    """Сверить одну домашнюю витрину с независимым ответом из источника."""
    hook = _clickhouse_hook()
    if not _mart_exists(hook, mart):
        raise AirflowException(
            f"витрина {mart} не найдена на узле подключения clickhouse_default; "
            f"проверьте имя и место создания витрины по заданию {lab_path}"
        )

    try:
        query = (SQL_ROOT / sql_name).read_text(encoding="utf-8")
        scope_query = (
            (SQL_ROOT / scope_sql_name).read_text(encoding="utf-8")
            if scope_sql_name is not None
            else None
        )
    except OSError as error:
        raise AirflowException(
            f"стенд не может прочитать запрос проверки {sql_name}: {error}; "
            "сообщите наставнику"
        ) from error

    try:
        parameter_sets = [None]
        if scope_query is not None:
            parameter_sets = [{"day": day} for (day,) in hook.get_records(scope_query)]
        summaries = [
            summary
            for parameters in parameter_sets
            for summary in hook.get_records(query, parameters=parameters)
        ]
    except Exception as error:
        raise AirflowException(
            f"не удалось проверить витрину {mart}: {error}; "
            f"сверьте имена и типы колонок с заданием {lab_path}. "
            "Подробности ошибки — в журнале этой задачи"
        ) from error

    failed_rows = sum(summary[3] for summary in summaries)
    if failed_rows == 0:
        return

    diagnostics = [detail for summary in summaries for detail in summary[4]][
        :DETAIL_LIMIT
    ]
    for detail in diagnostics:
        logging.error("%s: %s", mart, detail)
    days = "; ".join(
        f"{data_date}: ключей источника {reference_rows}, "
        f"ключей витрины {actual_rows}, расходятся {day_failed_rows}"
        for data_date, reference_rows, actual_rows, day_failed_rows, _ in summaries
        if day_failed_rows > 0
    )
    compared_sides = diagnostics[0]
    raise AirflowException(
        f"витрина {mart} расходится с источником: {days}. "
        f"Пример сравнения: {compared_sides}. Другие примеры — в журнале этой "
        f"задачи. Проверьте расчет для указанных ключей по заданию {lab_path}"
    )


@dag(
    dag_id="homework_check",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["dm", "labs"],
)
def homework_check():
    """Дать отдельный результат самопроверки по каждой домашней витрине."""
    check_mart.override(task_id="events_enriched")(
        "dm.events_enriched_v",
        "docs/labs/1-1-events-enriched.md",
        "events_enriched_vs_dds.sql",
        "events_enriched_scope.sql",
    )
    check_mart.override(task_id="top_pages_daily")(
        "dm.top_pages_daily_v",
        "docs/labs/1-2-top-pages-daily.md",
        "top_pages_daily_vs_dds.sql",
    )
    check_mart.override(task_id="session_overview")(
        "dm.session_overview_v",
        "docs/labs/1-3-session-overview.md",
        "session_overview_vs_dds.sql",
    )
    check_mart.override(task_id="dq_errors_daily")(
        "dm.dq_errors_daily_v",
        "docs/labs/1-4-dq-errors-daily.md",
        "dq_errors_daily_vs_ods.sql",
    )
    check_mart.override(task_id="utm_effectiveness")(
        "dm.utm_effectiveness_v",
        "docs/labs/1-5-utm-effectiveness.md",
        "utm_effectiveness_vs_dds.sql",
    )


homework_check()
