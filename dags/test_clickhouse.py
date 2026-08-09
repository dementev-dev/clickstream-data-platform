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

from airflow.sdk import Connection, dag, get_current_context, task

CLUSTER = "clickstream_cluster"
LOCAL_TABLE = "airflow_probe_local"
DISTRIBUTED_TABLE = "airflow_probe_distributed"
EXPECTED_TABLES = [
    (DISTRIBUTED_TABLE, "Distributed"),
    (LOCAL_TABLE, "ReplicatedMergeTree"),
]

# Ноду 2 пробник читает не своим подключением, а запросом remote() с ноды 1: у
# Airflow подготовлено одно подключение — к clickhouse-01, и второго ради
# пробника не заводят. При этом remote('clickhouse-02:9000', ...) делает
# инициатором распределённого запроса саму ноду 2 — проверяется именно это, а
# не доступность ноды 2 по сети. Порт 9000 — межсерверный, тогда как
# подключение Airflow ходит по HTTP на 8123.
NODES = (
    ("ноде 1", "system.tables"),
    ("ноде 2", "remote('clickhouse-02:9000', system.tables)"),
)


def _clickhouse_client():
    # clickhouse_connect импортируется внутри функции, а не наверху файла:
    # обработчик DAG разбирает этот файл снова и снова, и импорт наверху
    # оплачивался бы каждым разбором. Тяжёлые импорты Airflow советует
    # держать внутри задач.
    import clickhouse_connect

    connection = Connection.get("clickhouse_default")
    return clickhouse_connect.get_client(
        host=connection.host,
        port=connection.port,
        username=connection.login or "default",
        password=connection.password or "",
        database=connection.schema or "default",
        connect_timeout=5,
        send_receive_timeout=30,
    )


def _table_engines(client, source: str) -> list[tuple[str, str]]:
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
        f"DROP TABLE IF EXISTS default.{DISTRIBUTED_TABLE} ON CLUSTER {CLUSTER} SYNC"
    )
    client.command(
        f"DROP TABLE IF EXISTS default.{LOCAL_TABLE} ON CLUSTER {CLUSTER} SYNC"
    )


# Отсутствие таблиц проверяют обе задачи — перед созданием и после уборки,
# — поэтому у этой проверки своё имя, а остальные живут прямо в теле задач.
def _assert_tables_absent(client) -> None:
    for node_name, source in NODES:
        remaining = _table_engines(client, source)
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
        client = _clickhouse_client()
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
            for node_name, source in NODES:
                actual_tables = _table_engines(client, source)
                if actual_tables != EXPECTED_TABLES:
                    raise RuntimeError(
                        f"неверный набор таблиц на {node_name}: {actual_tables}"
                    )
        finally:
            client.close()

    @task
    def write_marker() -> dict[str, str]:
        client = _clickhouse_client()
        try:
            marker = f"{get_current_context()['run_id']}:{uuid.uuid4()}"
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
            if len(local_rows) != 1 or local_rows[0][1] != marker:
                raise RuntimeError(f"маркер не найден в локальной таблице: {marker}")
            return {"marker": marker, "hostname": local_rows[0][0]}
        finally:
            client.close()

    @task
    def read_from_node_2(written: dict[str, str]) -> None:
        client = _clickhouse_client()
        try:
            node_2_rows = client.query(
                """
                SELECT hostName()
                FROM remote('clickhouse-02:9000', system.one)
                """
            ).result_rows
            if len(node_2_rows) != 1:
                raise RuntimeError(f"не удалось определить имя ноды 2: {node_2_rows}")
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
                parameters={"marker": written["marker"]},
            ).result_rows
            if written["hostname"] == node_2_rows[0][0]:
                raise RuntimeError(
                    "запись и чтение маркера должны выполняться с разных нод"
                )
            expected_rows = [(1, written["hostname"], written["marker"])]
            if distributed_rows != expected_rows:
                raise RuntimeError(
                    "нода 2 не прочитала маркер первого шарда через Distributed: "
                    f"{written['marker']}, получено {distributed_rows}"
                )
        finally:
            client.close()

    # Уборка идёт только после успеха: упавший пробник оставляет кластер таким,
    # каким сломался, а остатки сносит начало следующего запуска. Правило
    # запуска решает здесь и то, что стенд увидит снаружи — с "all_done"
    # уборка стала бы зелёным концом графа и покрасила бы в зелёный запуск
    # с упавшей проверкой (ADR 0003).
    @task
    def cleanup_tables() -> None:
        client = _clickhouse_client()
        try:
            _drop_tables(client)
            _assert_tables_absent(client)
        finally:
            client.close()

    prepared = prepare_tables()
    written = write_marker()
    checked = read_from_node_2(written)

    prepared >> written >> checked >> cleanup_tables()


test_clickhouse()
