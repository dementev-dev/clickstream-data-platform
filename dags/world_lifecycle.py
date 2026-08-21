"""Создание и пересоздание прикладного мира.

Compose поднимает службы, а этот граф создаёт всё прикладное поверх них:
топики, схему ClickHouse, данные стартовых дней и позицию модельного времени.
`world_initialize` безопасен для обычного `make up`; `world_recreate` — явная
операция обслуживания, которая сначала объявляет прежний мир недействительным.

Контракт состояний и доводы — ADR 0013 и спека жизненного цикла мира.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import time
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.docker.hooks.docker import DockerHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Connection, Variable, dag, task

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
TAGS = ["жизненный цикл мира"]

WORLD_POSITION = "world_position"
STARTING_DAYS = int(os.environ["WORLD_STARTING_DAYS"])
HITS_TOPIC = os.environ["KAFKA_TOPIC"]
ORDERS_TOPIC = os.environ["KAFKA_ORDERS_TOPIC"]
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
GENERATOR_IMAGE = os.environ["GENERATOR_IMAGE"]
STAND_NETWORK = os.environ["STAND_NETWORK"]

SQL_ROOT = Path("/opt/airflow/sql/ddl")
WORLD_ROOT = Path("/opt/airflow/world")
INVENTORY = WORLD_ROOT / "data/world-inventory.json"
CLICKHOUSE_CONNECTION = "clickhouse_lifecycle"
CLICKHOUSE_CLUSTER = "clickstream_cluster"
APPLICATION_DATABASES = ("stg", "dm", "dds", "ods", "dic")
CONSUMER_GROUPS = ("clickstream_hits", "clickstream_orders")
TOPICS = {HITS_TOPIC: 2, ORDERS_TOPIC: 1}


def _clickhouse_client():
    """Открыть соединение владельца жизненного цикла."""
    import clickhouse_connect

    connection = Connection.get(CLICKHOUSE_CONNECTION)
    return clickhouse_connect.get_client(
        host=connection.host,
        port=connection.port,
        username=connection.login,
        password=connection.password,
        database=connection.schema or "default",
        connect_timeout=5,
        # `ON CLUSTER` ждёт хосты до 180 с. Транспорт живёт дольше, чтобы
        # ClickHouse сам назвал незавершённый хост вместо сетевого тайм-аута.
        send_receive_timeout=300,
    )


def _ddl_statements(text: str) -> list[str]:
    """Разделить канонический DDL по его явному правилу файлов.

    ClickHouse Connect исполняет один запрос за вызов. В `sql/ddl` конец
    выражения — точка с запятой в конце строки; это проверяемая конвенция
    наших файлов, а не попытка написать общий SQL-парсер.
    """
    statements: list[str] = []
    lines: list[str] = []
    for line in text.splitlines():
        lines.append(line)
        stripped = line.rstrip()
        if stripped.endswith(";") and not stripped.lstrip().startswith("--"):
            statement = "\n".join(lines).strip()
            statements.append(statement[:-1].rstrip())
            lines = []

    remainder = "\n".join(lines).strip()
    if remainder and any(
        line.strip() and not line.lstrip().startswith("--")
        for line in remainder.splitlines()
    ):
        raise AirflowException("DDL-файл оканчивается незавершённым выражением")
    return statements


def _generator(task_id: str, command: list[str]) -> DockerOperator:
    """Запустить канонический образ генератора в сети стенда."""
    return DockerOperator(
        task_id=task_id,
        image=GENERATOR_IMAGE,
        command=command,
        network_mode=STAND_NETWORK,
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": HITS_TOPIC,
        },
        # Вывод уже переехал в журнал задачи; завершённый контейнер не нужен.
        auto_remove="force",
        # Временный путь Airflow существует внутри контейнера, но монтировать
        # его пытается хостовый Docker. Генератору этот каталог не нужен.
        mount_tmp_dir=False,
    )


def _kafka_error_code(error: Exception) -> int:
    """Код ошибки из KafkaException."""
    return error.args[0].code()


@dag(
    dag_id="world_initialize",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=TAGS,
)
def world_initialize():
    """Создать стартовый мир, если готового мира ещё нет."""

    @task.branch
    def choose_path() -> str:
        raw_position = Variable.get(WORLD_POSITION, default=None)
        if raw_position is None:
            logging.info("world_position отсутствует: создаём первый мир")
            return "create_topics"
        try:
            position = int(raw_position)
        except (TypeError, ValueError) as error:
            raise AirflowException(
                f"world_position={raw_position!r} не является номером дня; "
                "выполните make rebuild-storage"
            ) from error
        if position == 0:
            logging.info("очистка завершена: создаём мир заново")
            return "create_topics"
        if position == -1:
            raise AirflowException(
                "world_position=-1: очистка мира не завершена; "
                "повторите make rebuild-storage"
            )
        if position >= STARTING_DAYS:
            logging.info("мир уже существует: world_position=%s", position)
            return "world_already_exists"
        raise AirflowException(
            f"world_position={position}: недопустимое незавершённое состояние; "
            "выполните make rebuild-storage"
        )

    @task
    def world_already_exists() -> None:
        """Читаемый зелёный конец ветки без изменений."""
        logging.info("инициализация не требуется: готовый мир не изменён")

    @task
    def create_topics() -> None:
        """Создать топики до появления читающих их таблиц ClickHouse."""
        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        futures = admin.create_topics(
            [
                NewTopic(name, num_partitions=partitions, replication_factor=1)
                for name, partitions in TOPICS.items()
            ],
            request_timeout=30,
            operation_timeout=15,
        )
        for name, future in futures.items():
            try:
                future.result()
                logging.info("топик %s создан", name)
            except KafkaException as error:
                if _kafka_error_code(error) != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
                logging.info("топик %s уже существует", name)

        metadata = admin.list_topics(timeout=15)
        for name, expected_partitions in TOPICS.items():
            topic = metadata.topics.get(name)
            actual_partitions = (
                len(topic.partitions) if topic and not topic.error else 0
            )
            if actual_partitions != expected_partitions:
                raise AirflowException(
                    f"у топика {name} разделов {actual_partitions}, "
                    f"ожидалось {expected_partitions}; "
                    "выполните make rebuild-storage"
                )

    @task
    def apply_ddl() -> None:
        """Применить канонические файлы DDL с ноды 1 по порядку имён."""
        files = sorted(SQL_ROOT.glob("*.sql"))
        if not files:
            raise AirflowException(f"в {SQL_ROOT} нет файлов DDL")

        client = _clickhouse_client()
        try:
            for path in files:
                statements = _ddl_statements(path.read_text(encoding="utf-8"))
                logging.info("применяем %s: выражений %s", path.name, len(statements))
                for statement in statements:
                    client.command(statement)
        finally:
            client.close()

    @task
    def build_generator_image() -> None:
        """Собрать генератор из текущего дерева, а не из старого образа."""
        client = DockerHook(
            docker_conn_id=None,
            base_url="unix://var/run/docker.sock",
            timeout=900,
        ).get_conn()
        output = client.build(
            path=str(WORLD_ROOT),
            dockerfile="generator/Dockerfile",
            tag=GENERATOR_IMAGE,
            rm=True,
            decode=True,
        )
        for item in output:
            if error := item.get("error"):
                raise AirflowException(
                    f"сборка генератора завершилась ошибкой: {error}"
                )
            if stream := item.get("stream", "").strip():
                logging.info("docker build: %s", stream)

    @task
    def wait_for_events() -> None:
        """Дождаться приёма всех событий описи, включая строки брака."""
        inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))
        expected = sum(day["events"] for day in inventory["days"])
        first_date = inventory["days"][0]["date"]
        last_date = inventory["days"][-1]["date"]
        query = f"""
            SELECT
                (SELECT count() FROM ods.event_dist
                    WHERE EventDate BETWEEN '{first_date}' AND '{last_date}')
              + (SELECT count() FROM ods.event_errors_dist)
        """

        client = _clickhouse_client()
        try:
            arrived = 0
            for attempt in range(1, 101):
                row = client.query(query).first_row
                if row is None:
                    raise AirflowException("ClickHouse не вернул счётчик приёма")
                arrived = int(row[0])
                if arrived >= expected:
                    logging.info("стартовый мир принят: строк %s", arrived)
                    return
                if attempt % 5 == 0:
                    logging.info("принято %s из %s строк", arrived, expected)
                time.sleep(3)
        finally:
            client.close()
        raise AirflowException(
            f"стартовый мир не принят за 300 с: строк {arrived} из {expected}; "
            "проверьте журнал send_initial_events и "
            "SELECT * FROM system.kafka_consumers"
        )

    @task
    def remember_starting_position() -> None:
        """Объявить мир готовым только после приёма обоих источников."""
        Variable.set(WORLD_POSITION, str(STARTING_DAYS))
        logging.info("мир готов: world_position=%s", STARTING_DAYS)

    route = choose_path()
    ready = world_already_exists()
    topics = create_topics()
    ddl = apply_ddl()
    image = build_generator_image()
    events = _generator(
        "send_initial_events",
        ["batch", "--day", "0", "--days", str(STARTING_DAYS)],
    )
    arrived = wait_for_events()
    snapshots = _generator(
        "send_initial_snapshots",
        [
            "snapshot",
            "--day",
            "0",
            "--days",
            str(STARTING_DAYS),
            "--topic",
            ORDERS_TOPIC,
        ],
    )
    orders = TriggerDagRunOperator(
        task_id="trigger_orders_ingest",
        trigger_dag_id="orders_ingest",
        wait_for_completion=True,
        poke_interval=10,
    )
    position = remember_starting_position()

    route >> [ready, topics]
    topics >> ddl >> image >> events >> arrived >> snapshots >> orders >> position


@dag(
    dag_id="world_recreate",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=TAGS,
)
def world_recreate():
    """Удалить только прикладной мир и делегировать его создание."""

    @task
    def invalidate_world() -> None:
        Variable.set(WORLD_POSITION, "-1")
        logging.info("прежний мир недействителен: world_position=-1")

    @task
    def drop_application_databases() -> None:
        client = _clickhouse_client()
        try:
            for database in APPLICATION_DATABASES:
                logging.info("удаляем базу %s", database)
                client.command(
                    f"DROP DATABASE IF EXISTS {database} "
                    f"ON CLUSTER {CLICKHOUSE_CLUSTER} SYNC"
                )
        finally:
            client.close()

    @task
    def delete_consumer_groups() -> None:
        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        remaining = set(CONSUMER_GROUPS)
        for attempt in range(1, 31):
            futures = admin.delete_consumer_groups(
                sorted(remaining), request_timeout=15
            )
            for group, future in futures.items():
                try:
                    future.result()
                    remaining.discard(group)
                    logging.info("группа %s удалена", group)
                except KafkaException as error:
                    code = _kafka_error_code(error)
                    if code == KafkaError.GROUP_ID_NOT_FOUND:
                        remaining.discard(group)
                    elif code != KafkaError.NON_EMPTY_GROUP:
                        raise
            if not remaining:
                return
            if attempt % 5 == 0:
                logging.info("ждём выхода потребителей из групп: %s", sorted(remaining))
            time.sleep(2)
        raise AirflowException(
            f"за 60 с не освободились группы потребителей: {sorted(remaining)}"
        )

    @task
    def delete_topics() -> None:
        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        futures = admin.delete_topics(list(TOPICS), request_timeout=30)
        for name, future in futures.items():
            try:
                future.result()
                logging.info("топик %s удалён", name)
            except KafkaException as error:
                if _kafka_error_code(error) != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise

        for _ in range(30):
            existing = set(admin.list_topics(timeout=10).topics) & set(TOPICS)
            if not existing:
                return
            time.sleep(1)
        raise AirflowException(f"топики не исчезли за 30 с: {sorted(existing)}")

    @task
    def mark_world_clean() -> None:
        Variable.set(WORLD_POSITION, "0")
        logging.info("очистка завершена: world_position=0")

    invalid = invalidate_world()
    databases = drop_application_databases()
    groups = delete_consumer_groups()
    topics = delete_topics()
    clean = mark_world_clean()
    initialize = TriggerDagRunOperator(
        task_id="trigger_world_initialize",
        trigger_dag_id="world_initialize",
        wait_for_completion=True,
        poke_interval=10,
    )

    invalid >> databases >> groups >> topics >> clean >> initialize


world_initialize()
world_recreate()
