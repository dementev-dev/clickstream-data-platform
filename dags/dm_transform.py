"""Трансформации слоя DM: сборка стендовых витрин.

Выручка наследует ритм заказов и заменяет дневные партиции изменяемого окна.
Трафик наследует ретроспективность карты идентичностей и пересчитывает всю
историю новой версией. Объем каждого прогона выводится из состояния слоев.

Форма DAG и динамическая замена сверены с публичным API Airflow 3 через
Context7; DDL и REPLACE PARTITION — с документацией ClickHouse.
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task, task_group

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

SQL_ROOT = Path("/opt/airflow/sql")
CLICKHOUSE_CONNECTION = "clickhouse_default"


def _days(first_day: datetime.date, last_day: datetime.date) -> list[str]:
    """Дни замены строками от левой до правой границы включительно."""
    return [
        (first_day + datetime.timedelta(days=shift)).isoformat()
        for shift in range((last_day - first_day).days + 1)
    ]


@task
def replacements(scope: dict[str, object]) -> list[dict[str, object]]:
    """Разложить дни в аргументы размноженной задачи замены.

    Отдельный словарь нужен каждому экземпляру ``expand_kwargs``; тот же
    прием разобран в ``dds_transform.py`` на замене партиций заказов.
    """
    return [{"params": {"day": day}} for day in scope["days"]]


@dag(
    dag_id="dm_transform",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=3,
    template_searchpath=str(SQL_ROOT),
    tags=["dm"],
)
def dm_transform():
    """Пересобрать стендовые витрины по текущему состоянию DDS."""

    @task_group(group_id="revenue_daily")
    def revenue_daily():
        """Выручка: изменяемое окно заказов и пропущенная история."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у хранилища, какие дни пересобирает запуск."""
            query = (SQL_ROOT / "dm" / "revenue_daily_scope.sql").read_text(
                encoding="utf-8"
            )
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query)
            if row is None:
                raise AirflowException(
                    "нет слепка заказов в ODS или готовых заказов в DDS"
                )
            first_day, last_day = row
            days = _days(first_day, last_day)
            logging.info(
                "объем пересборки: %s … %s, дней %d",
                first_day,
                last_day,
                len(days),
            )
            return {
                "first_day": first_day.isoformat(),
                "last_day": last_day.isoformat(),
                "days": days,
            }

        days = scope()

        rebuild = SQLExecuteQueryOperator(
            task_id="rebuild",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/revenue_daily_rebuild.sql",
            parameters={
                "load_id": "{{ run_id }}",
                "first_day": days["first_day"],
                "last_day": days["last_day"],
            },
            split_statements=True,
            do_xcom_push=False,
        )

        replace = SQLExecuteQueryOperator.partial(
            task_id="replace",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/revenue_daily_replace.sql",
            do_xcom_push=False,
            # Каждая замена поднимает отдельный процесс работника. Три
            # одновременные задачи выбили его по памяти на первом прогоне;
            # самим дням параллельность ничего не дает.
            max_active_tis_per_dag=1,
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    revenue_daily()

    SQLExecuteQueryOperator(
        task_id="daily_traffic",
        conn_id=CLICKHOUSE_CONNECTION,
        sql="dm/daily_traffic_rebuild.sql",
        parameters={"load_id": "{{ run_id }}"},
        do_xcom_push=False,
    )


dm_transform()
