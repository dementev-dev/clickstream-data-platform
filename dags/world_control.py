"""Пульт мира: даги, которыми двигают ось модельного времени.

Их три, и они двух родов. **Работники** играют день и запускаются руками:
`world_next_day` — пачкой, без пауз, сколько дней попросили; `world_live_day` —
один день в темпе модельного времени. **Выключатель** `world_live` своей работы
не делает: он тикает по расписанию и дёргает работника живого дня, дожидаясь
конца. Снят с паузы — мир едет день за днём; поставлен на паузу — встал на
границе модельных суток.

Сыграть день — половина работы. Вторая половина: отправить слепок заказов и
дождаться, пока хранилище его заберёт. Поэтому у обоих работников за проигрышем
идут отправка слепка тем же контейнером генератора и ждущий триггер дага приёма
`orders_ingest`.

Разделение не косметическое. Расписание на самом работнике заставило бы кнопку
паузы значить две вещи разом — «мир не едет сам» и «даг выключен», — а работник
при этом выглядел бы в списке выключенным, хотя нажимают его каждый день.

Календарь Airflow к оси мира отношения не имеет: какой день играть, работник
спрашивает у переменной, а не у логической даты прогона.

Решения и доводы целиком — ADR 0009.
"""

from __future__ import annotations

import datetime
import os

from airflow.exceptions import AirflowException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, Variable, dag, get_current_context, task

# Факты стенда — образ генератора, сеть, адрес брокера, топик и размер
# стартового мира — приходят окружением, и называет их compose: тот же, что
# называет их разовой службе генератора.
GENERATOR_IMAGE = os.environ["GENERATOR_IMAGE"]
STAND_NETWORK = os.environ["STAND_NETWORK"]
GENERATOR_ENVIRONMENT = {
    "KAFKA_BOOTSTRAP_SERVERS": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "KAFKA_TOPIC": os.environ["KAFKA_TOPIC"],
}
# Топик слепка называется аргументом, а не окружением: KAFKA_TOPIC выше — топик
# событий, и промолчи мы, заказы уехали бы к ним.
ORDERS_TOPIC = os.environ["KAFKA_ORDERS_TOPIC"]
STARTING_DAYS = int(os.environ["WORLD_STARTING_DAYS"])

# Позиция на оси: номер первого несыгранного дня. До конца инициализации
# работники не имеют права угадывать её — состояние объяснено в ADR 0013.
#
# Позиция ставится, а не увеличивается. Наложись один прогон на другой, худшее
# при таком правиле — сыгранный дважды день, а повтор схлопнет
# ReplacingMergeTree. Увеличение молча съело бы день, и в мире осталась бы
# дыра, которой никто не заметит.
WORLD_POSITION = "world_position"

# Тик выключателя. Каденцию задаёт не он, а сама длина живого дня — около
# двадцати четырёх минут: тик только спрашивает «не пора ли снова».
LIVE_TICK = datetime.timedelta(minutes=25)

# Какой день играть, работник узнаёт у первой задачи. Спрашивают её все
# генераторные шаги: слепок снимается с той же позиции, что и проигрыш.
PLAYED_DAY = "{{ ti.xcom_pull(task_ids='first_unplayed_day') }}"

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
TAGS = ["пульт мира"]


@task
def first_unplayed_day() -> int:
    """Номер дня, с которого играть."""
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
    if position < STARTING_DAYS:
        action = (
            "выполните make up" if position == 0 else "выполните make rebuild-storage"
        )
        raise AirflowException(f"мир не готов: world_position={position}; {action}")
    return position


def _generator(task_id: str, command: list[str]) -> DockerOperator:
    """Задача, зовущая генератор в его каноническом контейнере.

    Внутрь образа Airflow генератор не поставить: он требует Python 3.14, а
    образ несёт 3.13. Да и обещание побайтовой воспроизводимости дано для
    зафиксированного образа генератора — держится оно только там.
    """
    return DockerOperator(
        task_id=task_id,
        image=GENERATOR_IMAGE,
        command=command,
        network_mode=STAND_NETWORK,
        environment=GENERATOR_ENVIRONMENT,
        # Контейнер убирается в любом исходе: вывод генератора оператор уже
        # перелил в журнал задачи, а мёртвые контейнеры копить незачем.
        auto_remove="force",
        # По умолчанию оператор монтирует контейнеру временный каталог. Здесь
        # это ловушка: путь он заводит внутри Airflow, а монтирует демон с
        # хоста, где такого пути нет. Генератору временный каталог не нужен.
        mount_tmp_dir=False,
    )


def _ingest_orders() -> TriggerDagRunOperator:
    """Задача забора: дёрнуть даг приёма и дождаться, чем он кончился.

    Ожидание здесь несущее. Без него работник позеленел бы, не узнав, доехал
    ли слепок, и позиция мира ушла бы вперёд хранилища — а зелёный конец графа
    не должен переживать отказ выше (ADR 0003).
    """
    return TriggerDagRunOperator(
        task_id="trigger_orders_ingest",
        trigger_dag_id="orders_ingest",
        wait_for_completion=True,
        # Умолчание — минута опроса, а весь прогон работника пачкой короче.
        poke_interval=10,
    )


@dag(
    dag_id="world_next_day",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=TAGS,
    params={
        "days": Param(
            1, type="integer", minimum=1, maximum=7, title="Сколько дней прожить"
        )
    },
)
def world_next_day():
    """Прожить следующие дни пачкой, без пауз.

    Запускается руками. День по умолчанию один, но разгон вперёд идёт одним
    нажимом, а не десятью: сколько дней играть — параметр запуска.
    """

    @task
    def remember_played(first_day: int) -> None:
        """Позиция ставится по сыгранным дням и только по успеху."""
        days = get_current_context()["params"]["days"]
        Variable.set(WORLD_POSITION, str(first_day + days))

    first_day = first_unplayed_day()
    played = _generator(
        "play_days",
        ["batch", "--day", PLAYED_DAY, "--days", "{{ params.days }}"],
    )
    # Разгон на N дней отправляет N слепков — по одному за сыгранный день, и
    # каждый со своим сдвигом: прогон дня D везёт слепок дня D−1. Забор при
    # этом остаётся один.
    sent = _generator(
        "send_snapshots",
        [
            "snapshot",
            "--day",
            PLAYED_DAY,
            "--days",
            "{{ params.days }}",
            "--topic",
            ORDERS_TOPIC,
        ],
    )

    first_day >> played >> sent >> _ingest_orders() >> remember_played(first_day)


@dag(
    dag_id="world_live_day",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=TAGS,
)
def world_live_day():
    """Прожить следующий день в темпе модельного времени.

    Ускорение ×60: модельные сутки укладываются примерно в двадцать четыре
    реальные минуты, и суточная волна разворачивается на глазах. Запускается
    руками; чтобы мир жил так день за днём сам, есть выключатель `world_live`.
    """

    @task
    def remember_played(first_day: int) -> None:
        """Позиция ставится по сыгранному дню и только по успеху."""
        Variable.set(WORLD_POSITION, str(first_day + 1))

    first_day = first_unplayed_day()
    played = _generator("play_day", ["live", "--day", PLAYED_DAY])
    # Слепок уезжает пачкой и после дня, а не в темпе: у выгрузки бэкенда темпа
    # нет вовсе — она снимается на границе суток целиком.
    sent = _generator(
        "send_snapshot",
        ["snapshot", "--day", PLAYED_DAY, "--topic", ORDERS_TOPIC],
    )

    first_day >> played >> sent >> _ingest_orders() >> remember_played(first_day)


@dag(
    dag_id="world_live",
    schedule=LIVE_TICK,
    start_date=START_DATE,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=TAGS,
)
def world_live():
    """Выключатель: пока включён, мир живёт день за днём.

    Своей работы у выключателя нет — он дёргает `world_live_day` и ждёт конца.
    Ожидание тут несущая конструкция, а не вежливость: без него тик шёл бы
    независимо от хода дня, лишние прогоны скопились бы очередью, и мир потом
    промчался бы по ней без всякого темпа.

    Ждём триггером, а не сенсором: оператор опрашивает тот прогон, который сам
    и создал, и ссылка на дочерний прогон видна прямо отсюда. Упал день —
    краснеет и выключатель.
    """
    TriggerDagRunOperator(
        task_id="trigger_live_day",
        trigger_dag_id="world_live_day",
        wait_for_completion=True,
    )


world_next_day()
world_live_day()
world_live()
