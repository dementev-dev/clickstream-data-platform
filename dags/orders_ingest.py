"""Приём заказов: пакетный забор слепка из Kafka в хранилище.

Путь Kafka → STG → ODS у заказов один и живёт одним дагом; здесь его первый
шаг — забор. Расписания у дага нет: его дёргает тот, кто положил слепок в
топик, — работник пульта мира после проигрыша дня, — и ждёт конца.

Контраст с приёмом событий и есть урок. События тянет матвью, навсегда
подписанная на чтеца: приём идёт сам, пока идёт поток. Слепок заказов
приезжает раз в модельный день целой выгрузкой, у которой есть начало и конец,
и забирает её запрос по команде. Push против pull — два режима на одном стенде,
каждый там, где ему место по природе источника.

Решения и доводы целиком — ADR 0008; форма перехода STG → ODS, который
прирастёт сюда вторым шагом, — docs/architecture/orders/ingestion.md.
"""

from __future__ import annotations

import datetime
import logging

from airflow.sdk import Connection, dag, get_current_context, task

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

# Забор — один прямой SELECT, без цикла до пустоты: одна порция ClickHouse
# берёт десятки тысяч сообщений, а слепок дня — порядка полутора тысяч строк.
# Короткая порция оставит хвост до следующего прогона, а отказ после чтения
# унесёт прочитанное с собой: офсеты коммитятся в момент чтения. Граница
# целиком — ADR 0008, «Следствия».
#
# stream_like_engine_allow_direct_select разрешает читать чтеца запросом; вторая
# половина пары объявлена на самой таблице (sql/ddl/10-stg-tables.sql).
# distributed_foreground_insert = 1 — конвенция ETL-вставок стенда: задача не
# должна зеленеть раньше, чем строки легли на шарды.
TAKE_ONE_BATCH = """
INSERT INTO stg.orders_raw_dist
SELECT
    raw,
    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    _timestamp_ms AS kafka_timestamp,
    hostName() AS consumer_host,
    {load_id:String} AS _load_id,
    now64(3) AS _load_ts
FROM stg.orders_raw_kafka
SETTINGS
    stream_like_engine_allow_direct_select = 1,
    distributed_foreground_insert = 1
"""


@dag(
    dag_id="orders_ingest",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    # Чтец у топика один, и группа потребителей у него одна. Два прогона разом
    # дрались бы за неё, а слепок разъехался бы по двум _load_id; второй
    # прогон подождёт своей очереди.
    max_active_runs=1,
    tags=["заказы"],
)
def orders_ingest():
    """Забрать приехавший слепок заказов из топика в сырьё."""

    @task
    def pull_batch() -> None:
        # Импорт внутри задачи: обработчик DAG разбирает этот файл снова и
        # снова, и импорт наверху оплачивался бы каждым разбором.
        import clickhouse_connect

        load_id = get_current_context()["run_id"]
        connection = Connection.get("clickhouse_default")
        client = clickhouse_connect.get_client(
            host=connection.host,
            port=connection.port,
            username=connection.login,
            password=connection.password,
            database=connection.schema or "default",
            connect_timeout=5,
            send_receive_timeout=30,
        )
        try:
            summary = client.command(TAKE_ONE_BATCH, parameters={"load_id": load_id})
        finally:
            client.close()
        # Размер порции — read_rows: written_rows у вставки в Distributed
        # считает не приехавшее.
        logging.info(
            "порция принята: строк %s, _load_id %s",
            summary.summary["read_rows"],
            load_id,
        )

    pull_batch()


orders_ingest()
