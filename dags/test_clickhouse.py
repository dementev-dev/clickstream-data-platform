"""Сквозная проверка подключения Airflow к кластеру ClickHouse."""

from __future__ import annotations

import datetime
import uuid

from airflow.sdk import Connection, dag, get_current_context, task

CLUSTER = "clickstream_cluster"
LOCAL_TABLE = "airflow_probe_local"
DISTRIBUTED_TABLE = "airflow_probe_distributed"


def _table_engines(client, *, query_node_2: bool) -> list[tuple[str, str]]:
    source = (
        "remote('clickhouse-02:9000', system.tables)"
        if query_node_2
        else "system.tables"
    )
    result = client.query(
        f"""
        SELECT name, engine
        FROM {source}
        WHERE database = 'default'
          AND name IN ('{LOCAL_TABLE}', '{DISTRIBUTED_TABLE}')
        ORDER BY name
        """
    )
    return result.result_rows


def _drop_tables(client) -> None:
    client.command(
        f"DROP TABLE IF EXISTS default.{DISTRIBUTED_TABLE} "
        f"ON CLUSTER {CLUSTER} SYNC"
    )
    client.command(
        f"DROP TABLE IF EXISTS default.{LOCAL_TABLE} ON CLUSTER {CLUSTER} SYNC"
    )


def _assert_tables_absent(client) -> None:
    for query_node_2, node_name in ((False, "ноде 1"), (True, "ноде 2")):
        remaining = _table_engines(client, query_node_2=query_node_2)
        if remaining:
            raise RuntimeError(
                f"служебные таблицы остались на {node_name}: {remaining}"
            )


def _assert_marker_path(
    *,
    local_rows: list[tuple[str, str]],
    distributed_rows: list[tuple[int, str, str]],
    node_2_hostname: str,
    marker: str,
) -> None:
    if len(local_rows) != 1 or local_rows[0][1] != marker:
        raise RuntimeError(f"маркер не найден в локальной таблице: {marker}")
    node_1_hostname = local_rows[0][0]
    if node_1_hostname == node_2_hostname:
        raise RuntimeError("запись и чтение маркера должны выполняться с разных нод")
    expected_rows = [(1, node_1_hostname, marker)]
    if distributed_rows != expected_rows:
        raise RuntimeError(
            "нода 2 не прочитала маркер первого шарда через Distributed: "
            f"{marker}, получено {distributed_rows}"
        )


def _cleanup_clickhouse_client(client, original_error: BaseException | None) -> None:
    cleanup_errors: list[Exception] = []
    try:
        _drop_tables(client)
        _assert_tables_absent(client)
    except Exception as error:
        cleanup_errors.append(error)
    try:
        client.close()
    except Exception as error:
        cleanup_errors.append(error)

    if original_error is not None:
        for error in cleanup_errors:
            original_error.add_note(
                f"Дополнительная ошибка очистки ClickHouse: {error}"
            )
        return
    if len(cleanup_errors) == 1:
        raise cleanup_errors[0]
    if cleanup_errors:
        raise ExceptionGroup("ошибки очистки ClickHouse", cleanup_errors)


@dag(
    dag_id="test_clickhouse",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["проверка"],
    doc_md=__doc__,
)
def test_clickhouse():
    @task
    def check_cluster_path() -> None:
        import clickhouse_connect

        connection = Connection.get("clickhouse_default")
        client = clickhouse_connect.get_client(
            host=connection.host,
            port=connection.port,
            username=connection.login or "default",
            password=connection.password or "",
            database=connection.schema or "default",
            connect_timeout=5,
            send_receive_timeout=30,
        )
        marker = f"{get_current_context()['run_id']}:{uuid.uuid4()}"
        expected_tables = [
            (DISTRIBUTED_TABLE, "Distributed"),
            (LOCAL_TABLE, "ReplicatedMergeTree"),
        ]
        probe_error = None

        try:
            _drop_tables(client)
            _assert_tables_absent(client)
            client.command(
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
                """
            )
            client.command(
                f"""
                CREATE TABLE default.{DISTRIBUTED_TABLE} ON CLUSTER {CLUSTER}
                AS default.{LOCAL_TABLE}
                ENGINE = Distributed(
                    '{CLUSTER}',
                    'default',
                    '{LOCAL_TABLE}',
                    cityHash64(marker)
                )
                """
            )

            for query_node_2, node_name in ((False, "ноде 1"), (True, "ноде 2")):
                actual_tables = _table_engines(
                    client,
                    query_node_2=query_node_2,
                )
                if actual_tables != expected_tables:
                    raise RuntimeError(
                        f"неверный набор таблиц на {node_name}: {actual_tables}"
                    )

            client.insert(
                f"default.{LOCAL_TABLE}",
                [[marker]],
                column_names=["marker"],
            )
            local_rows = client.query(
                f"""
                SELECT hostName(), marker
                FROM default.{LOCAL_TABLE}
                WHERE marker = {{marker:String}}
                """,
                parameters={"marker": marker},
            ).result_rows
            node_2_rows = client.query(
                """
                SELECT hostName()
                FROM remote('clickhouse-02:9000', system.one)
                """
            ).result_rows
            if len(node_2_rows) != 1:
                raise RuntimeError(
                    f"не удалось определить имя ноды 2: {node_2_rows}"
                )
            distributed_rows = client.query(
                f"""
                SELECT _shard_num, hostName(), marker
                FROM remote(
                    'clickhouse-02:9000',
                    'default',
                    '{DISTRIBUTED_TABLE}'
                )
                WHERE marker = {{marker:String}}
                """,
                parameters={"marker": marker},
            ).result_rows
            _assert_marker_path(
                local_rows=local_rows,
                distributed_rows=distributed_rows,
                node_2_hostname=node_2_rows[0][0],
                marker=marker,
            )
        except BaseException as error:
            probe_error = error
            raise
        finally:
            _cleanup_clickhouse_client(client, probe_error)

    check_cluster_path()


test_clickhouse()
