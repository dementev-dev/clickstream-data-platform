"""Трансформации слоя DM: сборка стендовых витрин.

Выручка наследует ритм заказов. Сверка соединяет окно заказов с прожитым
хвостом событий. Обе заменяют дневные партиции. Трафик наследует
ретроспективность карты идентичностей и пересчитывает всю историю новой
версией. Объем каждого прогона выводится из состояния слоев.
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable, dag, task, task_group

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

SQL_ROOT = Path("/opt/airflow/sql")
CLICKHOUSE_CONNECTION = "clickhouse_default"
# Та же ось модельного времени, что у DDS. Общего модуля у дагов нет
# намеренно: связь держит переменная Airflow, а не импорт одного дага другим.
WORLD_POSITION = "world_position"


def _world_position() -> int:
    """Позиция мира для границы прожитых событий сверки.

    Причина отдельного чтения переменной разобрана у одноименной функции в
    ``dds_transform.py``.
    """
    raw_position = Variable.get(WORLD_POSITION, default=None)
    if raw_position is None:
        raise AirflowException(
            "мир еще не создан: выполните make up и дождитесь world_initialize"
        )
    try:
        position = int(raw_position)
    except (TypeError, ValueError) as error:
        raise AirflowException(
            f"world_position={raw_position!r} не является номером дня; "
            "выполните make rebuild-storage"
        ) from error
    if position < 1:
        raise AirflowException(
            f"мир не готов: world_position={position}; "
            "выполните make up или make rebuild-storage"
        )
    return position


def _days(first_day: datetime.date, last_day: datetime.date) -> list[str]:
    """Дни отрезка строками - в таком виде их ждет замена партиции."""
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
                    "нет слепка заказов в ODS или готовых заказов в DDS; "
                    "дождитесь загрузки слепка и сначала выполните dds_transform"
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
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    revenue_daily()

    @task_group(group_id="purchase_vs_orders")
    def purchase_vs_orders():
        """Сверка: окно заказов, прожитый хвост и предварительные дни."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у обоих источников, какие дни пересобрать."""
            query = (SQL_ROOT / "dm" / "purchase_vs_orders_scope.sql").read_text(
                encoding="utf-8"
            )
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query, parameters={"position": _world_position()})
            if row is None:
                raise AirflowException(
                    "нет принятого слепка заказов или прожитых покупок; "
                    "дождитесь источников и сначала выполните dds_transform"
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
            sql="dm/purchase_vs_orders_rebuild.sql",
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
            sql="dm/purchase_vs_orders_replace.sql",
            do_xcom_push=False,
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    purchase_vs_orders()

    SQLExecuteQueryOperator(
        task_id="daily_traffic",
        conn_id=CLICKHOUSE_CONNECTION,
        sql="dm/daily_traffic_rebuild.sql",
        parameters={"load_id": "{{ run_id }}"},
        do_xcom_push=False,
    )


dm_transform()
