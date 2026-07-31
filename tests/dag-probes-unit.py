"""Малые проверки логики пробника ClickHouse без запуска Airflow."""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


class DeclaredTask:
    """Заглушка объявленной задачи: держит только цепочку через `>>`."""

    def __rshift__(self, other):
        return other


def load_clickhouse_dag_tasks():
    airflow_module = types.ModuleType("airflow")
    sdk_module = types.ModuleType("airflow.sdk")
    captured_tasks = {}

    def dag(**_kwargs):
        def decorate(function):
            return function

        return decorate

    def task(function=None, **_kwargs):
        def capture(target):
            captured_tasks[target.__name__] = target

            def declare_task(*_args, **_kwargs):
                return DeclaredTask()

            return declare_task

        return capture(function) if function is not None else capture

    sdk_module.Connection = object
    sdk_module.dag = dag
    sdk_module.get_current_context = lambda: {}
    sdk_module.task = task
    airflow_module.sdk = sdk_module
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = sdk_module

    dag_path = Path(__file__).resolve().parents[1] / "dags" / "test_clickhouse.py"
    spec = importlib.util.spec_from_file_location("test_clickhouse_dag", dag_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("не удалось загрузить модуль пробника ClickHouse")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, captured_tasks


def load_kafka_dag_task():
    airflow_module = types.ModuleType("airflow")
    sdk_module = types.ModuleType("airflow.sdk")
    captured_tasks = {}

    def dag(**_kwargs):
        def decorate(function):
            return function

        return decorate

    def task(function):
        captured_tasks[function.__name__] = function

        def declare_task(*_args, **_kwargs):
            return None

        return declare_task

    sdk_module.dag = dag
    sdk_module.get_current_context = lambda: {"run_id": "unit-test"}
    sdk_module.task = task
    airflow_module.sdk = sdk_module
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = sdk_module

    dag_path = Path(__file__).resolve().parents[1] / "dags" / "test_kafka.py"
    spec = importlib.util.spec_from_file_location("test_kafka_dag", dag_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("не удалось загрузить модуль пробника Kafka")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return captured_tasks["check_round_trip"]


class QueryResult:
    def __init__(self, result_rows: list[tuple]) -> None:
        self.result_rows = result_rows


class RecordingClient:
    """Клиент ClickHouse, который запоминает запросы и отвечает заготовкой."""

    def __init__(self, remaining_tables: list[tuple[str, str]] | None = None) -> None:
        self.commands: list[str] = []
        self.queries: list[str] = []
        self.closed = False
        self._remaining_tables = remaining_tables or []

    def command(self, sql: str) -> None:
        self.commands.append(" ".join(sql.split()))

    def query(self, sql: str, parameters=None) -> QueryResult:
        self.queries.append(" ".join(sql.split()))
        return QueryResult(list(self._remaining_tables))

    def close(self) -> None:
        self.closed = True


class ClickHouseProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module, cls.tasks = load_clickhouse_dag_tasks()

    def run_cleanup_task(self, client: RecordingClient) -> None:
        original_client_factory = self.module._clickhouse_client
        self.module._clickhouse_client = lambda: client
        try:
            self.tasks["cleanup_tables"]()
        finally:
            self.module._clickhouse_client = original_client_factory

    def test_marker_path_requires_different_nodes_and_first_shard(self) -> None:
        marker = "свой-маркер"
        self.module._assert_marker_path(
            distributed_rows=[(1, "clickhouse-01-host", marker)],
            write_hostname="clickhouse-01-host",
            node_2_hostname="clickhouse-02-host",
            marker=marker,
        )

        with self.assertRaisesRegex(RuntimeError, "разных нод"):
            self.module._assert_marker_path(
                distributed_rows=[(2, "clickhouse-02-host", marker)],
                write_hostname="clickhouse-02-host",
                node_2_hostname="clickhouse-02-host",
                marker=marker,
            )
        with self.assertRaisesRegex(RuntimeError, "первого шарда"):
            self.module._assert_marker_path(
                distributed_rows=[(2, "clickhouse-01-host", marker)],
                write_hostname="clickhouse-01-host",
                node_2_hostname="clickhouse-02-host",
                marker=marker,
            )

    def test_cleanup_drops_tables_and_checks_both_nodes(self) -> None:
        client = RecordingClient()

        self.run_cleanup_task(client)

        dropped = {
            table
            for table in (self.module.LOCAL_TABLE, self.module.DISTRIBUTED_TABLE)
            if any(
                command.startswith(f"DROP TABLE IF EXISTS default.{table} ")
                for command in client.commands
            )
        }
        self.assertEqual(
            dropped, {self.module.LOCAL_TABLE, self.module.DISTRIBUTED_TABLE}
        )
        self.assertEqual(len(client.queries), len(self.module.NODES))
        self.assertTrue(client.closed)

    def test_cleanup_closes_client_when_tables_survive(self) -> None:
        client = RecordingClient(remaining_tables=[("airflow_probe_local", "Log")])

        with self.assertRaisesRegex(RuntimeError, "служебные таблицы остались"):
            self.run_cleanup_task(client)

        self.assertTrue(client.closed)


class KafkaProbeTests(unittest.TestCase):
    def test_producer_flushes_when_consumer_creation_fails(self) -> None:
        kafka_module = types.ModuleType("confluent_kafka")
        flush_timeouts = []

        class Message:
            def partition(self) -> int:
                return 0

            def offset(self) -> int:
                return 1

        class Producer:
            def __init__(self, _config) -> None:
                pass

            def produce(self, _topic, **kwargs) -> None:
                kwargs["on_delivery"](None, Message())

            def flush(self, timeout: int) -> int:
                flush_timeouts.append(timeout)
                return 0

        class Consumer:
            def __init__(self, _config) -> None:
                raise RuntimeError("чтение недоступно")

        kafka_module.Consumer = Consumer
        kafka_module.Producer = Producer
        kafka_module.TopicPartition = object
        sys.modules["confluent_kafka"] = kafka_module
        check_round_trip = load_kafka_dag_task()

        with self.assertRaisesRegex(RuntimeError, "чтение недоступно"):
            check_round_trip()

        self.assertEqual(flush_timeouts, [10, 1])


def run_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    result = unittest.TestResult()
    suite.run(result)

    problems = result.failures + result.errors
    for test, details in problems:
        print(f"ОШИБКА: {test.id()}", file=sys.stderr)
        print(details, file=sys.stderr)
    passed = result.testsRun - len(problems) - len(result.skipped)
    print(f"ИТОГ: пройдено {passed}, ошибок {len(problems)}")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(run_tests())
