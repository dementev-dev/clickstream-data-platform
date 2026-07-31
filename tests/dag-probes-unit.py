"""Малые проверки логики пробника ClickHouse без запуска Airflow."""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


def load_clickhouse_dag_module():
    airflow_module = types.ModuleType("airflow")
    sdk_module = types.ModuleType("airflow.sdk")

    def dag(**_kwargs):
        def decorate(function):
            return function

        return decorate

    def task(function):
        def declare_task(*_args, **_kwargs):
            return None

        return declare_task

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
    return module


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


class FailingCleanupClient:
    def __init__(self) -> None:
        self.closed = False

    def command(self, _sql: str) -> None:
        raise RuntimeError("очистка недоступна")

    def close(self) -> None:
        self.closed = True


class ClickHouseProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_clickhouse_dag_module()

    def test_marker_path_requires_different_nodes_and_first_shard(self) -> None:
        marker = "свой-маркер"
        self.module._assert_marker_path(
            local_rows=[("clickhouse-01-host", marker)],
            distributed_rows=[(1, "clickhouse-01-host", marker)],
            node_2_hostname="clickhouse-02-host",
            marker=marker,
        )

        with self.assertRaisesRegex(RuntimeError, "разных нод"):
            self.module._assert_marker_path(
                local_rows=[("clickhouse-02-host", marker)],
                distributed_rows=[(2, "clickhouse-02-host", marker)],
                node_2_hostname="clickhouse-02-host",
                marker=marker,
            )
        with self.assertRaisesRegex(RuntimeError, "первого шарда"):
            self.module._assert_marker_path(
                local_rows=[("clickhouse-01-host", marker)],
                distributed_rows=[(2, "clickhouse-01-host", marker)],
                node_2_hostname="clickhouse-02-host",
                marker=marker,
            )

    def test_cleanup_keeps_original_error_as_primary(self) -> None:
        client = FailingCleanupClient()
        original_error = RuntimeError("маркер не найден")

        self.module._cleanup_clickhouse_client(client, original_error)

        self.assertTrue(client.closed)
        self.assertIn("очистка недоступна", "\n".join(original_error.__notes__))


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
