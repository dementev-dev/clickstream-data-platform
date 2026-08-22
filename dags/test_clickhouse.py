"""Сквозная проверка связности Airflow с кластером ClickHouse.

Тест проверяет:

- до запуска служебных таблиц нет ни на одной ноде;
- после создания на обеих лежит ожидаемая пара движков;
- маркер запуска вставлен в локальную таблицу ноды 1 и найден там ровно одной
  строкой;
- нода 2 читает этот маркер через Distributed и видит его на первом шарде;
- после уборки таблиц не осталось ни на одной ноде.

Этот тест — не образец ETL. Он ходит ON CLUSTER на каждом шаге, заводит и
сносит собственные служебные таблицы за один запуск и читает ноду 2 запросом
remote(): это приёмы проверки связности, а не загрузки данных.
"""

from __future__ import annotations

import datetime
import uuid

from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.sdk import Connection, dag, get_current_context, task

CLUSTER = "clickstream_cluster"
LOCAL_TABLE = "airflow_probe_local"
DISTRIBUTED_TABLE = "airflow_probe_distributed"
EXPECTED_TABLES = [
    (DISTRIBUTED_TABLE, "Distributed"),
    (LOCAL_TABLE, "ReplicatedMergeTree"),
]

# У Airflow одно подключение — к ноде 1; второго ради пробника не заводят.
# Ноду 2 он читает через remote(), который не использует секрет из описания
# кластера, поэтому учётные данные etl передаются явно. Порт 9000 — нативный,
# тогда как подключение Airflow ходит по HTTP на 8123. Trace-журнал сервера
# видит пароль: это допустимо только для локального учебного стенда.
NODES = (
    ("ноде 1", "system.tables"),
    (
        "ноде 2",
        """remote(
            'clickhouse-02:9000', 'system', 'tables',
            {remote_user:String}, {remote_password:String}
        )""",
    ),
)


def _remote_parameters() -> dict[str, str]:
    connection = Connection.get("clickhouse_default")
    return {
        "remote_user": connection.login,
        "remote_password": connection.password,
    }


def _clickhouse_hook() -> ClickHouseHook:
    return ClickHouseHook(clickhouse_conn_id="clickhouse_default")


def _table_engines(hook: ClickHouseHook, source: str) -> list[tuple[str, str]]:
    return hook.get_records(
        f"""
        SELECT name, engine
        FROM {source}
        WHERE database = 'default'
          AND name IN ('{LOCAL_TABLE}', '{DISTRIBUTED_TABLE}')
        ORDER BY name
        """,
        parameters=_remote_parameters(),
    )


def _drop_tables(hook: ClickHouseHook) -> None:
    hook.run(
        [
            f"DROP TABLE IF EXISTS default.{DISTRIBUTED_TABLE} "
            f"ON CLUSTER {CLUSTER} SYNC",
            f"DROP TABLE IF EXISTS default.{LOCAL_TABLE} ON CLUSTER {CLUSTER} SYNC",
        ],
    )


# Отсутствие таблиц проверяют обе задачи — перед созданием и после уборки,
# — поэтому у этой проверки своё имя, а остальные живут прямо в теле задач.
def _assert_tables_absent(hook: ClickHouseHook) -> None:
    for node_name, source in NODES:
        remaining = _table_engines(hook, source)
        if remaining:
            raise RuntimeError(
                f"служебные таблицы остались на {node_name}: {remaining}"
            )


@dag(
    dag_id="test_clickhouse",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    catchup=False,
    tags=["проверка"],
    doc_md=__doc__,
)
def test_clickhouse():
    @task
    def prepare_tables() -> None:
        hook = _clickhouse_hook()
        _drop_tables(hook)
        _assert_tables_absent(hook)
        hook.run(
            [
                f"""
                CREATE TABLE default.{LOCAL_TABLE} ON CLUSTER {CLUSTER}
                (
                    marker String
                )
                ENGINE = ReplicatedMergeTree(
                    '/clickhouse/tables/{{shard}}/{LOCAL_TABLE}',
                    '{{replica}}'
                )
                ORDER BY marker
                """,
                f"""
                CREATE TABLE default.{DISTRIBUTED_TABLE} ON CLUSTER {CLUSTER}
                AS default.{LOCAL_TABLE}
                ENGINE = Distributed(
                    '{CLUSTER}',
                    'default',
                    '{LOCAL_TABLE}',
                    cityHash64(marker)
                )
                """,
            ],
        )
        for node_name, source in NODES:
            actual_tables = _table_engines(hook, source)
            if actual_tables != EXPECTED_TABLES:
                raise RuntimeError(
                    f"неверный набор таблиц на {node_name}: {actual_tables}"
                )

    @task
    def write_marker() -> dict[str, str]:
        hook = _clickhouse_hook()
        marker = f"{get_current_context()['run_id']}:{uuid.uuid4()}"
        hook.bulk_insert_rows(
            f"default.{LOCAL_TABLE}",
            [[marker]],
            column_names=["marker"],
        )
        local_rows = hook.get_records(
            f"""
            SELECT hostName(), marker
            FROM default.{LOCAL_TABLE}
            WHERE marker = {{marker:String}}
            """,
            parameters={"marker": marker},
        )
        if len(local_rows) != 1 or local_rows[0][1] != marker:
            raise RuntimeError(f"маркер не найден в локальной таблице: {marker}")
        return {"marker": marker, "hostname": local_rows[0][0]}

    @task
    def read_from_node_2(written: dict[str, str]) -> None:
        hook = _clickhouse_hook()
        node_2_rows = hook.get_records(
            """
            SELECT hostName()
            FROM remote(
                'clickhouse-02:9000', 'system', 'one',
                {remote_user:String}, {remote_password:String}
            )
            """,
            parameters=_remote_parameters(),
        )
        if len(node_2_rows) != 1:
            raise RuntimeError(f"не удалось определить имя ноды 2: {node_2_rows}")
        distributed_rows = hook.get_records(
            f"""
            SELECT _shard_num, hostName(), marker
            FROM remote(
                'clickhouse-02:9000',
                'default',
                '{DISTRIBUTED_TABLE}',
                {{remote_user:String}},
                {{remote_password:String}}
            )
            WHERE marker = {{marker:String}}
            """,
            parameters={
                "marker": written["marker"],
                **_remote_parameters(),
            },
        )
        if written["hostname"] == node_2_rows[0][0]:
            raise RuntimeError("запись и чтение маркера должны идти с разных нод")
        expected_rows = [(1, written["hostname"], written["marker"])]
        if distributed_rows != expected_rows:
            raise RuntimeError(
                "нода 2 не прочитала маркер первого шарда через Distributed: "
                f"{written['marker']}, получено {distributed_rows}"
            )

    # Уборка идёт только после успеха: упавший пробник оставляет кластер таким,
    # каким сломался, а остатки сносит начало следующего запуска. Правило
    # запуска решает здесь и то, что стенд увидит снаружи — с "all_done"
    # уборка стала бы зелёным концом графа и покрасила бы в зелёный запуск
    # с упавшей проверкой (ADR 0003).
    @task
    def cleanup_tables() -> None:
        hook = _clickhouse_hook()
        _drop_tables(hook)
        _assert_tables_absent(hook)

    prepared = prepare_tables()
    written = write_marker()
    checked = read_from_node_2(written)

    prepared >> written >> checked >> cleanup_tables()


test_clickhouse()
