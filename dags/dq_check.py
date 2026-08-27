"""Проверки качества стендовых моделей и витрин.

Пять проверок сначала собирают общий донор. После его полной сборки дневные
партиции публикуются одной серией замен, и только затем именованные задачи
утверждают результат. Расхождение поэтому остаётся в ``dm.dq_summary_v`` до
красного завершения дага; ошибка сборки прежнюю сводку не меняет.
"""

from __future__ import annotations

import datetime
import json
import logging
from collections import defaultdict
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable, dag, get_current_context, task, task_group

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

SQL_ROOT = Path("/opt/airflow/sql")
INVENTORY_PATH = Path("/opt/airflow/world/data/world-inventory.json")
CLICKHOUSE_CONNECTION = "clickhouse_default"
WORLD_POSITION = "world_position"
DETAIL_LIMIT = 20

SUMMARY_COLUMNS = [
    "data_date",
    "check_name",
    "reference_rows",
    "actual_rows",
    "failed_rows",
    "status",
    "_load_id",
    "_load_ts",
]


def _clickhouse_hook() -> ClickHouseHook:
    """Подключение, которое дожидается записи в Distributed-таблицу."""
    return ClickHouseHook(
        clickhouse_conn_id=CLICKHOUSE_CONNECTION,
        session_settings={"distributed_foreground_insert": 1},
    )


def _world_position() -> int:
    """Первый несыгранный день оси мира."""
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


def _write_summary(
    check_name: str,
    comparisons: list[tuple[object, ...]],
) -> list[str]:
    """Свернуть строки деловых ключей в дневную сводку и малый срез ошибок."""
    if not comparisons:
        raise AirflowException(f"{check_name}: область проверки пуста")

    totals: dict[datetime.date, list[int]] = defaultdict(lambda: [0, 0, 0])
    diagnostics: list[str] = []
    for (
        data_date,
        business_key,
        reference_present,
        actual_present,
        reference_value,
        actual_value,
        failed,
    ) in comparisons:
        counts = totals[data_date]
        counts[0] += int(reference_present)
        counts[1] += int(actual_present)
        counts[2] += int(failed)
        if failed and len(diagnostics) < DETAIL_LIMIT:
            reference_text = reference_value if reference_present else "нет ключа"
            actual_text = actual_value if actual_present else "нет ключа"
            diagnostics.append(
                f"{data_date} key={business_key}: "
                f"эталон={reference_text}, объект={actual_text}"
            )

    context = get_current_context()
    load_id = context["run_id"]
    load_ts = datetime.datetime.now(datetime.UTC)
    rows = [
        (
            data_date,
            check_name,
            reference_rows,
            actual_rows,
            failed_rows,
            "pass" if failed_rows == 0 else "fail",
            load_id,
            load_ts,
        )
        for data_date, (reference_rows, actual_rows, failed_rows) in sorted(
            totals.items()
        )
    ]
    _clickhouse_hook().bulk_insert_rows(
        "dm.dq_summary_stage_dist",
        rows,
        column_names=SUMMARY_COLUMNS,
    )
    return diagnostics


@task
def build_sql_check(check_name: str, sql_name: str) -> list[str]:
    """Выполнить одну SQL-сверку и записать ее дневные итоги в донор."""
    query = (SQL_ROOT / "dq" / sql_name).read_text(encoding="utf-8")
    parameters = (
        {"position": _world_position()}
        if check_name == "sessions_vs_reference"
        else None
    )
    comparisons = _clickhouse_hook().get_records(query, parameters=parameters)
    return _write_summary(check_name, comparisons)


@task
def build_classes_vs_inventory() -> list[str]:
    """Сопоставить классы закрытых дней с техническим оракулом генератора."""
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    expected = {
        (datetime.date.fromisoformat(day["date"]), mismatch_class): count
        for day in inventory["days"]
        if "orders" in day
        for mismatch_class, count in day["orders"].items()
    }
    if not expected:
        raise AirflowException("classes_vs_inventory: в описи нет закрытых дней")

    query = (SQL_ROOT / "dq" / "classes_vs_inventory.sql").read_text(encoding="utf-8")
    actual = {
        (data_date, mismatch_class): count
        for data_date, mismatch_class, count in _clickhouse_hook().get_records(query)
        if (data_date, mismatch_class) in expected
    }
    comparisons = [
        (
            data_date,
            mismatch_class,
            1,
            int((data_date, mismatch_class) in actual),
            reference_count,
            actual.get((data_date, mismatch_class)),
            reference_count != actual.get((data_date, mismatch_class)),
        )
        for (data_date, mismatch_class), reference_count in sorted(expected.items())
    ]
    return _write_summary("classes_vs_inventory", comparisons)


@task
def publication_scope() -> list[dict[str, dict[str, str]]]:
    """Вернуть дни полностью собранного донора для размноженной замены."""
    rows = _clickhouse_hook().get_records(
        "SELECT DISTINCT data_date FROM dm.dq_summary_stage_dist ORDER BY data_date"
    )
    if not rows:
        raise AirflowException("донор dm.dq_summary пуст: публиковать нечего")
    return [{"params": {"day": data_date.isoformat()}} for (data_date,) in rows]


@task
def drop_stale_partitions() -> None:
    """Убрать дни, которые после полного прогона не покрывает ни одна сторона."""
    hook = _clickhouse_hook()
    stale_days = hook.get_records(
        """
        SELECT DISTINCT data_date
        FROM dm.dq_summary_dist
        WHERE data_date GLOBAL NOT IN
            (SELECT data_date FROM dm.dq_summary_stage_dist)
        ORDER BY data_date
        """
    )
    for (data_date,) in stale_days:
        # ALTER PARTITION не принимает параметр ClickHouse. Значение пришло
        # из колонки Date самого хранилища и форматируется без внешнего ввода.
        hook.run(
            "ALTER TABLE dm.dq_summary_rep ON CLUSTER clickstream_cluster "
            f"DROP PARTITION '{data_date.isoformat()}'"
        )


@task
def assert_check(check_name: str, diagnostics: list[str]) -> None:
    """Покрасить именованную задачу по уже опубликованной сводке."""
    failed_days = _clickhouse_hook().get_records(
        """
        SELECT data_date, reference_rows, actual_rows, failed_rows
        FROM dm.dq_summary_v
        WHERE check_name = {check_name:String} AND failed_rows > 0
        ORDER BY data_date
        """,
        parameters={"check_name": check_name},
    )
    if not failed_days:
        return

    for detail in diagnostics:
        logging.error("%s: %s", check_name, detail)
    summary = "; ".join(
        f"{data_date}: эталонных ключей {reference_rows}, "
        f"фактических {actual_rows}, нарушено {failed_rows}"
        for data_date, reference_rows, actual_rows, failed_rows in failed_days
    )
    raise AirflowException(f"{check_name}: {summary}")


@dag(
    dag_id="dq_check",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    # Три одновременные сверки на живом мире исчерпали 640 МиБ scheduler и
    # получили SIGKILL. Последовательность здесь дешева: весь даг укладывается
    # в один учебный прогон, а общему донору параллельность ничего не даёт.
    max_active_tasks=1,
    template_searchpath=str(SQL_ROOT),
    tags=["dq"],
)
def dq_check():
    """Собрать, опубликовать и утвердить текущее качество данных."""
    prepare = SQLExecuteQueryOperator(
        task_id="prepare",
        conn_id=CLICKHOUSE_CONNECTION,
        sql="dq/prepare.sql",
        do_xcom_push=False,
    )

    sessions = build_sql_check.override(task_id="build_sessions_vs_reference")(
        "sessions_vs_reference", "sessions_vs_reference.sql"
    )
    classes = build_classes_vs_inventory.override(
        task_id="build_classes_vs_inventory"
    )()
    revenue = build_sql_check.override(task_id="build_revenue_daily_vs_dds")(
        "revenue_daily_vs_dds", "revenue_daily_vs_dds.sql"
    )
    purchases = build_sql_check.override(task_id="build_purchase_vs_orders_vs_dds")(
        "purchase_vs_orders_vs_dds", "purchase_vs_orders_vs_dds.sql"
    )
    traffic = build_sql_check.override(task_id="build_daily_traffic_vs_dds")(
        "daily_traffic_vs_dds", "daily_traffic_vs_dds.sql"
    )
    builders = [sessions, classes, revenue, purchases, traffic]
    prepare >> builders

    days = publication_scope()
    for builder in builders:
        builder >> days

    replace = SQLExecuteQueryOperator.partial(
        task_id="publish",
        conn_id=CLICKHOUSE_CONNECTION,
        sql="dq/replace.sql",
        do_xcom_push=False,
        # Каждая замена — отдельный процесс LocalExecutor. Последовательность
        # держит даг в измеренном лимите памяти планировщика.
        max_active_tis_per_dag=1,
    ).expand_kwargs(days)

    sessions_assertion = assert_check.override(task_id="sessions_vs_reference")(
        "sessions_vs_reference", sessions
    )
    classes_assertion = assert_check.override(task_id="classes_vs_inventory")(
        "classes_vs_inventory", classes
    )

    @task_group(group_id="marts_vs_dds")
    def assert_marts():
        return [
            assert_check.override(task_id="revenue_daily_vs_dds")(
                "revenue_daily_vs_dds", revenue
            ),
            assert_check.override(task_id="purchase_vs_orders_vs_dds")(
                "purchase_vs_orders_vs_dds", purchases
            ),
            assert_check.override(task_id="daily_traffic_vs_dds")(
                "daily_traffic_vs_dds", traffic
            ),
        ]

    assertions = [sessions_assertion, classes_assertion, *assert_marts()]
    remove_stale = drop_stale_partitions()
    replace >> remove_stale >> assertions


dq_check()
