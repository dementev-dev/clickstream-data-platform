"""Пульт мира: даги, которыми двигают ось модельного времени.

Их три, и они двух родов. **Работники** играют день и запускаются руками:
`world_next_day` — пачкой, без пауз, сколько дней попросили; `world_live_day` —
один день в темпе модельного времени. **Выключатель** `world_live` своей работы
не делает: он тикает по расписанию и дёргает работника живого дня, дожидаясь
конца. Снят с паузы — мир едет день за днём; поставлен на паузу — встал на
границе модельных суток.

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

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, Variable, dag, get_current_context, task

# Факты стенда — образ генератора, сеть, адрес брокера, топик и размер
# стартового мира — приходят окружением, и называет их compose: тот же, что
# называет их разовой службе генератора. Генератор для пульта — отдельная и
# заменяемая сущность, и знает о нём даг ровно то, что здесь перечислено.
GENERATOR_IMAGE = os.environ["GENERATOR_IMAGE"]
STAND_NETWORK = os.environ["STAND_NETWORK"]
GENERATOR_ENVIRONMENT = {
    "KAFKA_BOOTSTRAP_SERVERS": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "KAFKA_TOPIC": os.environ["KAFKA_TOPIC"],
}
STARTING_DAYS = int(os.environ["WORLD_STARTING_DAYS"])

# Позиция на оси: номер первого несыгранного дня. Переменной нет — мир в
# стартовом состоянии, и играть надо сразу за ним.
#
# Позиция именно ставится, а не увеличивается на единицу. Наложись один прогон
# на другой, худшее при таком правиле — сыгранный дважды день: номера событий
# детерминированы, и повтор схлопнет ReplacingMergeTree. Увеличение в том же
# случае молча съело бы день, и в мире осталась бы дыра, которой никто не
# заметит.
WORLD_POSITION = "world_position"

# Тик выключателя. Каденцию задаёт не он, а сама длина живого дня — около
# двадцати четырёх минут: тик только спрашивает «не пора ли снова».
LIVE_TICK = datetime.timedelta(minutes=25)

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
TAGS = ["пульт мира"]


@task
def first_unplayed_day() -> int:
    """Номер дня, с которого играть."""
    return int(Variable.get(WORLD_POSITION, default=STARTING_DAYS))


def _play(task_id: str, command: list[str]) -> DockerOperator:
    """Задача, играющая дни в каноническом контейнере генератора.

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
        # Контейнер убирается за собой в любом исходе — вопреки имени
        # значения: оператор сносит его в `finally`. Терять при этом нечего,
        # вывод генератора он уже перелил в журнал задачи.
        auto_remove="success",
        # По умолчанию оператор монтирует контейнеру временный каталог. Здесь
        # это ловушка: путь он заводит внутри Airflow, а монтирует демон с
        # хоста, где такого пути нет. Генератору временный каталог не нужен.
        mount_tmp_dir=False,
    )


@dag(
    dag_id="world_next_day",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=TAGS,
    params={"days": Param(1, type="integer", minimum=1, title="Сколько дней прожить")},
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
    played = _play(
        "play_days",
        [
            "batch",
            "--day",
            "{{ ti.xcom_pull(task_ids='first_unplayed_day') }}",
            "--days",
            "{{ params.days }}",
        ],
    )

    first_day >> played >> remember_played(first_day)


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
    played = _play(
        "play_day",
        ["live", "--day", "{{ ti.xcom_pull(task_ids='first_unplayed_day') }}"],
    )

    first_day >> played >> remember_played(first_day)


@dag(
    dag_id="world_live",
    schedule=LIVE_TICK,
    start_date=START_DATE,
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
