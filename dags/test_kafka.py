"""Сквозная проверка записи и чтения сообщения Airflow через Kafka.

Тест проверяет:

- брокер принял маркер запуска и подтвердил его ровно одним сообщением;
- вместе с подтверждением брокер вернул адрес записи — раздел и смещение;
- чтение по этому адресу возвращает записанный маркер;
- ошибка брокера при чтении — отказ;
- молчание брокера дольше срока — отказ.

Запись и чтение — две задачи, потому что ломаются они по-разному: в интерфейсе
Airflow сразу видно, брокер не принял маркер или не отдал его обратно. Адрес
записи едет от первой задачи ко второй обычным XCom.

Этот тест — не образец боевого потребителя. Он не подписывается на топик, не
входит в группу, не коммитит смещения и не создаёт топик: сообщение читается
по адресу, который брокер назвал при записи. Это приёмы проверки связности, а
не работы с потоком.
"""

from __future__ import annotations

import datetime
import time
import uuid
from typing import NamedTuple

from airflow.sdk import dag, get_current_context, task

BROKER = "kafka:9092"
# Топик пробника постоянный, и стенд опирается на автосоздание: срок хранения
# чистит записи, а не топик. В бою автосоздание обычно выключают.
TOPIC = "airflow_integration_probe"

# Сроки ожидания у записи меряют разное, и путать их дорого: DELIVERY —
# сколько библиотека держит сообщение у себя, пока пробует его доставить;
# SOCKET — сколько ждём ответа сети на один запрос; FLUSH — сколько ждём, пока
# библиотека вернёт управление и скажет, чем доставка кончилась.
DELIVERY_TIMEOUT_MS = 10_000
SOCKET_TIMEOUT_MS = 5_000
FLUSH_TIMEOUT_SEC = 10

PRODUCER_CONFIG = {
    "bootstrap.servers": BROKER,
    "client.id": "airflow-test-kafka",
    "message.timeout.ms": DELIVERY_TIMEOUT_MS,
    "socket.timeout.ms": SOCKET_TIMEOUT_MS,
}

# Группа у чтения одноразовая, автокоммит выключен: пробник не двигает ничьих
# смещений и не оставляет следов на брокере. Обойтись совсем без группы нельзя
# — без group.id библиотека консьюмера не создаст.
CONSUMER_GROUP_PREFIX = "airflow-probe-"
CONSUMER_CONFIG = {
    "bootstrap.servers": BROKER,
    "enable.auto.commit": False,
    "socket.timeout.ms": SOCKET_TIMEOUT_MS,
}

READ_DEADLINE_SEC = 20
POLL_TIMEOUT_SEC = 0.5


class RecordAddress(NamedTuple):
    """Адрес записи в топике, каким его называет брокер в подтверждении.

    Тип живёт внутри задачи записи: два соседних целых в сигнатуре
    переставляются молча, а имена полей этого не дают. Через границу задач
    адрес едет отдельными полями словаря — XCom проходит через JSON, и кортеж
    вернулся бы на той стороне списком.
    """

    partition: int
    offset: int


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
    def write_marker() -> dict[str, str | int]:
        # confluent_kafka импортируется внутри задачи, а не наверху файла:
        # обработчик DAG разбирает этот файл снова и снова, и импорт наверху
        # оплачивался бы каждым разбором. Тяжёлые импорты Airflow советует
        # держать внутри задач.
        from confluent_kafka import Producer

        marker = f"{get_current_context()['run_id']}:{uuid.uuid4()}"
        payload = marker.encode()
        delivery_errors: list[str] = []
        addresses: list[RecordAddress] = []

        def remember_delivery(error, message) -> None:
            if error is not None:
                delivery_errors.append(str(error))
            else:
                addresses.append(
                    RecordAddress(message.partition(), message.offset())
                )

        producer = Producer(PRODUCER_CONFIG)
        # produce() не пишет, а ставит сообщение в очередь: о судьбе записи
        # сообщает колбэк, а гарантию даёт flush() — он же возвращает число
        # недоставленных и он же единственный способ закрыть за собой продюсера.
        # Раздел выбирается по ключу сообщения, а маркер уникален для запуска,
        # поэтому адрес записи мы не выбираем, а узнаём от брокера.
        try:
            producer.produce(
                TOPIC,
                key=payload,
                value=payload,
                on_delivery=remember_delivery,
            )
        finally:
            undelivered = producer.flush(FLUSH_TIMEOUT_SEC)

        if undelivered:
            raise RuntimeError(
                f"Kafka не приняла маркер за {FLUSH_TIMEOUT_SEC} с, "
                f"не доставлено сообщений {undelivered}: {marker}"
            )
        if delivery_errors:
            raise RuntimeError(
                f"Kafka отказалась принять маркер {marker}: {delivery_errors}"
            )
        if len(addresses) != 1:
            raise RuntimeError(
                f"Kafka подтвердила запись маркера {marker} "
                f"не одним сообщением: {addresses}"
            )
        return {
            "marker": marker,
            "partition": addresses[0].partition,
            "offset": addresses[0].offset,
        }

    @task
    def read_marker(written: dict[str, str | int]) -> None:
        from confluent_kafka import Consumer, TopicPartition

        marker = str(written["marker"])
        consumer = Consumer(
            {**CONSUMER_CONFIG, "group.id": f"{CONSUMER_GROUP_PREFIX}{uuid.uuid4()}"}
        )
        try:
            # Пробник назначает себе известный адрес, а не подписывается на
            # топик: адрес брокер назвал при записи, поэтому ни группа, которая
            # делит разделы между читателями, ни перебалансировка чтению не
            # нужны.
            consumer.assign(
                [TopicPartition(TOPIC, written["partition"], written["offset"])]
            )
            deadline = time.monotonic() + READ_DEADLINE_SEC
            message = None
            # None от poll() — норма, а не отказ: брокер отвечает молчанием и
            # когда сообщения нет, и когда оно ещё не доехало. Отличить одно от
            # другого нечем, поэтому у чтения обязан быть крайний срок.
            while message is None and time.monotonic() < deadline:
                message = consumer.poll(POLL_TIMEOUT_SEC)
            if message is None:
                raise RuntimeError(
                    f"Kafka молчала {READ_DEADLINE_SEC} с и не вернула маркер: {marker}"
                )
            if message.error():
                raise RuntimeError(f"Kafka вернула ошибку чтения: {message.error()}")
            if message.value() != marker.encode():
                raise RuntimeError(
                    f"по адресу записи лежит не маркер запуска {marker}: "
                    f"{message.value()!r}"
                )
        finally:
            consumer.close()

    written = write_marker()
    read_marker(written)


test_kafka()
