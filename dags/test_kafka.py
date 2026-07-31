"""Проверка записи и чтения сообщения Airflow через Kafka."""

from __future__ import annotations

import datetime
import time
import uuid

from airflow.sdk import dag, get_current_context, task

BROKER = "kafka:9092"
# Топик пробника постоянный, и стенд опирается на автосоздание: срок хранения
# чистит записи, а не топик. В бою автосоздание обычно выключают.
TOPIC = "airflow_integration_probe"


@dag(
    dag_id="test_kafka",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["проверка"],
    doc_md=__doc__,
)
def test_kafka():
    @task
    def check_round_trip() -> None:
        from confluent_kafka import Consumer, Producer, TopicPartition

        marker = f"{get_current_context()['run_id']}:{uuid.uuid4()}"
        marker_bytes = marker.encode()
        group_id = f"airflow-probe-{uuid.uuid4()}"
        producer = None
        consumer = None

        try:
            delivery_errors: list[str] = []
            delivered_offsets: list[tuple[int, int]] = []

            def on_delivery(error, message) -> None:
                if error is not None:
                    delivery_errors.append(str(error))
                else:
                    delivered_offsets.append((message.partition(), message.offset()))

            producer = Producer(
                {
                    "bootstrap.servers": BROKER,
                    "client.id": "airflow-test-kafka",
                    "message.timeout.ms": 10_000,
                    "socket.timeout.ms": 5_000,
                }
            )
            producer.produce(
                TOPIC,
                key=marker_bytes,
                value=marker_bytes,
                on_delivery=on_delivery,
            )
            undelivered = producer.flush(10)
            if undelivered or delivery_errors or len(delivered_offsets) != 1:
                raise RuntimeError(
                    "Kafka не подтвердила запись маркера: "
                    f"не доставлено {undelivered}, ошибки {delivery_errors}, "
                    f"смещения {delivered_offsets}"
                )

            consumer = Consumer(
                {
                    "bootstrap.servers": BROKER,
                    "group.id": group_id,
                    "enable.auto.commit": False,
                    "session.timeout.ms": 6_000,
                    "socket.timeout.ms": 5_000,
                }
            )
            partition, offset = delivered_offsets[0]
            consumer.assign([TopicPartition(TOPIC, partition, offset)])
            read_deadline = time.monotonic() + 20
            while time.monotonic() < read_deadline:
                message = consumer.poll(0.5)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(f"Kafka вернула ошибку чтения: {message.error()}")
                if message.value() == marker_bytes:
                    return
            raise RuntimeError(f"Kafka не вернула свой маркер за 20 секунд: {marker}")
        finally:
            if producer is not None:
                producer.flush(1)
            if consumer is not None:
                consumer.close()

    check_round_trip()


test_kafka()
