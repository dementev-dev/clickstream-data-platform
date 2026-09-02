"""Трансформации слоя DDS: сборка моделей.

Заказ и сессия собираются заменой дневных партиций. Карта идентичностей
пересчитывается целиком после заказа, потому что читает его текущую модель.

Расписания у дага нет и параметров тоже: объём каждого прогона он выводит сам
из состояния слоёв и позиции мира, поэтому один и тот же запуск одинаково
верен из триггера дневного конвейера, с кнопки менти и после падения
(docs/architecture/etl.md, «Объём прогона»).

Заказ и сессия гасят повтор заменой партиции целиком: день собирается в
таблицу-двойник и подменяет собой партицию цели. Карта пишет полный набор пар
новой версией, а точное чтение даёт FINAL. Замена партиции разобрана в
sql/dds/order_replace.sql; точное чтение карты описано в
sql/ddl/50-dds-views.sql.
"""

from __future__ import annotations

import datetime
import logging
import os
import time
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable, dag, task, task_group

# Пропуск приезжает из Task SDK: в airflow.exceptions он объявлен устаревшим
# и говорит об этом в журнал задачи (проверено на 3.3.0).
from airflow.sdk.exceptions import AirflowSkipException

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

SQL_ROOT = Path("/opt/airflow/sql")
CLICKHOUSE_CONNECTION = "clickhouse_default"
CLICKHOUSE_CLUSTER = "clickstream_cluster"

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
HITS_TOPIC = os.environ["KAFKA_TOPIC"]
HITS_GROUP = "clickstream_hits"
HITS_PARTITIONS = (0, 1)

READINESS_TIMEOUT_SECONDS = 300
READINESS_POLL_SECONDS = 3

# Позиция на оси модельного времени: номер первого несыгранного дня. Её ставят
# даги модельного времени, и только она отличает прожитый день от живого — зачем это
# сессиям, разобрано в sql/dds/session_scope.sql.
WORLD_POSITION = "world_position"


def _kafka_readiness() -> tuple[bool, str]:
    """Проверить, дочитала ли группа оба раздела событий."""
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    groups = admin.list_consumer_groups(request_timeout=10).result()
    if groups.errors:
        raise RuntimeError(f"Kafka не вернула список групп: {groups.errors}")

    group_exists = any(group.group_id == HITS_GROUP for group in groups.valid)
    committed: dict[int, int] = {}
    if group_exists:
        request = ConsumerGroupTopicPartitions(
            HITS_GROUP,
            [TopicPartition(HITS_TOPIC, partition) for partition in HITS_PARTITIONS],
        )
        result = admin.list_consumer_group_offsets([request], request_timeout=10)[
            HITS_GROUP
        ].result()
        for topic_partition in result.topic_partitions:
            if topic_partition.error is None and topic_partition.offset >= 0:
                committed[topic_partition.partition] = topic_partition.offset

    high_watermarks: dict[int, int] = {}
    watermark_errors: dict[int, str] = {}
    # OffsetSpec.latest возвращает следующее смещение после хвоста; сверено
    # через Context7 и на установленном confluent-kafka 2.15.0.
    latest = admin.list_offsets(
        {
            TopicPartition(HITS_TOPIC, partition): OffsetSpec.latest()
            for partition in HITS_PARTITIONS
        },
        request_timeout=10,
    )
    for topic_partition, future in latest.items():
        try:
            offset = future.result().offset
            if offset >= 0:
                high_watermarks[topic_partition.partition] = offset
        except Exception as error:
            watermark_errors[topic_partition.partition] = str(error)

    all_known = group_exists
    total_lag = 0
    parts: list[str] = []
    for partition in HITS_PARTITIONS:
        offset = committed.get(partition)
        high = high_watermarks.get(partition)
        lag = high - offset if offset is not None and high is not None else None
        if lag is None or lag < 0:
            all_known = False
        else:
            total_lag += lag
        logging.info(
            "Kafka %s[%s]: подтверждено=%s, верхнее смещение=%s, отставание=%s",
            HITS_TOPIC,
            partition,
            offset,
            high,
            lag,
        )
        parts.append(
            f"{HITS_TOPIC}[{partition}]: подтверждено={offset}, "
            f"верхнее смещение={high}, отставание={lag}"
        )
        if partition in watermark_errors:
            parts[-1] += (
                f", ошибка чтения верхнего смещения={watermark_errors[partition]}"
            )

    if not group_exists:
        logging.info("группа потребителей %s отсутствует", HITS_GROUP)
    lag_text = str(total_lag) if all_known else "неизвестно"
    logging.info("суммарное отставание Kafka: %s", lag_text)
    detail = "; ".join(parts)
    if not group_exists:
        detail = f"группа {HITS_GROUP} отсутствует; {detail}"
    return all_known and total_lag == 0, detail


def _delivery_queue_readiness() -> tuple[bool, str]:
    """Проверить, пуста ли очередь доставки событий на каждой ноде."""
    query = f"""
        SELECT
            node,
            sum(queued_files) AS files,
            arrayStringConcat(
                groupArrayIf(
                    last_exception,
                    queued_files > 0 AND notEmpty(last_exception)
                ),
                '; '
            ) AS last_exception
        FROM
        (
            -- У пустой очереди нет строк; system.one оставляет обе ноды
            -- видимыми и в зеленом опросе.
            SELECT
                hostName() AS node,
                toUInt64(0) AS queued_files,
                '' AS last_exception
            FROM clusterAllReplicas('{CLICKHOUSE_CLUSTER}', system.one)

            UNION ALL

            SELECT
                hostName(),
                data_files + broken_data_files AS queued_files,
                last_exception
            FROM clusterAllReplicas(
                '{CLICKHOUSE_CLUSTER}', system.distribution_queue
            )
            WHERE database = 'ods' AND table = 'event_dist'
        )
        GROUP BY node
        ORDER BY node
    """
    rows = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION).get_records(query)
    ready = True
    nodes: list[str] = []
    for node, files, last_exception in rows:
        files = int(files)
        ready = ready and files == 0
        logging.info(
            "очередь ods.event_dist на %s: файлов=%s, последняя ошибка=%s",
            node,
            files,
            last_exception or "нет",
        )
        detail = f"{node}: файлов={files}"
        if last_exception:
            detail += f", последняя ошибка={last_exception}"
        nodes.append(detail)
    return ready, "; ".join(nodes)


def _wait_for_phase(
    probe,
    phase: str,
    deadline: float,
) -> None:
    """Дождаться прохождения одного барьера до общего крайнего срока."""
    last_observation = "первый опрос не выполнен"
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            ready, last_observation = probe()
            last_error = None
        except Exception as error:
            ready = False
            last_error = str(error)
            logging.warning("%s: ошибка опроса: %s", phase, last_error)
        if ready:
            return

        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(READINESS_POLL_SECONDS, remaining))

    detail = last_observation
    if last_error:
        detail += f"; ошибка последнего опроса: {last_error}"
    raise AirflowException(
        f"общий срок ожидания готовности в {READINESS_TIMEOUT_SECONDS} с "
        f'истек на этапе "{phase}": {detail}'
    )


def _world_position() -> int:
    """Позиция мира числом, с внятным отказом на непригодном значении.

    Ту же переменную читают даги времени, но общего модуля у дагов нет намеренно:
    связь между дагами времени и трансформацией держит сама переменная, а не импорт.
    """
    raw_position = Variable.get(WORLD_POSITION, default=None)
    if raw_position is None:
        raise AirflowException(
            "мир ещё не создан: выполните make up и дождитесь world_initialize"
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
    """Дни отрезка строками — в таком виде их ждёт замена партиции."""
    return [
        (first_day + datetime.timedelta(days=shift)).isoformat()
        for shift in range((last_day - first_day).days + 1)
    ]


@task
def replacements(scope: dict[str, object]) -> list[dict[str, object]]:
    """Разложить дни в аргументы размноженной задачи замены.

    Шаг существует ради ограничения Airflow: размножать задачу можно
    только по целому возврату другой задачи — ни по ключу словаря, ни по
    карте поверх него. Отсюда отдельный возврат под один список.

    День едет в запрос подстановкой Jinja, а не параметром ClickHouse:
    в ALTER … PARTITION сервер параметр не подставляет — замерено,
    отвечает синтаксической ошибкой. Значение при этом не внешнее, его
    вернул сам ClickHouse запросом объёма.
    """
    return [{"params": {"day": day}} for day in scope["days"]]


@dag(
    dag_id="dds_transform",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    # Два прогона разом собирали бы один и тот же двойник: второй затёр бы
    # чужую сборку между вставкой и заменой партиций.
    max_active_runs=1,
    template_searchpath=str(SQL_ROOT),
    tags=["dds"],
)
def dds_transform():
    """Пересобрать сущности DDS по текущему состоянию источников."""

    @task
    def wait_for_source() -> None:
        """Дождаться чтения событий из Kafka и их доставки в ODS."""
        deadline = time.monotonic() + READINESS_TIMEOUT_SECONDS
        _wait_for_phase(_kafka_readiness, "Kafka", deadline)
        _wait_for_phase(
            _delivery_queue_readiness,
            "очередь доставки ClickHouse",
            deadline,
        )

    @task_group(group_id="order")
    def order():
        """Заказ: дышащее окно плюс дни, которых в слое ещё нет."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у хранилища, какие дни собирает этот запуск."""
            query = (SQL_ROOT / "dds" / "order_scope.sql").read_text(encoding="utf-8")
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query)
            # Пустой ответ означает пустой источник: собирать нечего, и
            # зеленеть на этом нельзя — прогон встаёт с внятной причиной.
            if row is None:
                raise AirflowException(
                    "в ods.order нет ни одного заказа: собирать нечего; "
                    "проверьте, дошёл ли слепок до хранилища"
                )
            first_day, last_day = row
            days = _days(first_day, last_day)
            logging.info(
                "объём пересборки: %s … %s, дней %d",
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
            sql="dds/order_rebuild.sql",
            parameters={
                "load_id": "{{ run_id }}",
                "first_day": days["first_day"],
                "last_day": days["last_day"],
            },
            split_statements=True,
            do_xcom_push=False,
        )

        # Замена идёт по партиции за раз — другой формы у операции нет. День
        # становится отдельной задачей: в интерфейсе видно, сколько дней несёт
        # прогон, а повтор после отказа стоит одного дня, а не всей пересборки.
        replace = SQLExecuteQueryOperator.partial(
            task_id="replace",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dds/order_replace.sql",
            do_xcom_push=False,
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    @task_group(group_id="session")
    def session():
        """Сессия: дни, которые мир уже прожил, а слой ещё не собрал."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у хранилища и у позиции мира, какие дни собирает запуск."""
            query = (SQL_ROOT / "dds" / "session_scope.sql").read_text(encoding="utf-8")
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query, parameters={"position": _world_position()})
            if row is None:
                raise AirflowException(
                    "в ods.event нет ни одного события: собирать нечего; "
                    "проверьте, дошёл ли поток до хранилища"
                )
            first_day, last_day = row
            # Пропуск, а не отказ: слой полон по последний прожитый день, а
            # сегодняшний ещё идёт. Это обычное состояние повторного прогона,
            # и в интерфейсе оно видно пропущенной группой.
            if first_day is None:
                raise AirflowSkipException(
                    f"сессии собраны по {last_day} включительно, "
                    "новых прожитых дней нет"
                )
            days = _days(first_day, last_day)
            logging.info(
                "объём сборки: %s … %s, дней %d",
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
            sql="dds/session_rebuild.sql",
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
            sql="dds/session_replace.sql",
            do_xcom_push=False,
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    source_ready = wait_for_source()
    orders = order()
    sessions = session()
    source_ready >> [orders, sessions]

    identity_map = SQLExecuteQueryOperator(
        task_id="identity_map",
        conn_id=CLICKHOUSE_CONNECTION,
        sql="dds/identity_map_rebuild.sql",
        parameters={"load_id": "{{ run_id }}"},
        do_xcom_push=False,
    )

    orders >> identity_map


dds_transform()
