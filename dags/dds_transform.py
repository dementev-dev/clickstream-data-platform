"""Трансформации слоя DDS: сборка модели заменой дневных партиций.

Даг собирает модель поверх принятого ODS. Сущностей у него две — заказ и
сессия, зависимости по данным между ними нет; карта идентичностей встанет
соседней группой задач.

Расписания у дага нет и параметров тоже: объём каждого прогона он выводит сам
из состояния слоёв и позиции мира, поэтому один и тот же запуск одинаково
верен из триггера дневного конвейера, с кнопки менти и после падения
(docs/architecture/etl.md, «Объём прогона»).

Повтор прогона гасит не колонка версии, а замена партиции целиком: день
собирается в таблицу-двойник и подменяет собой партицию цели. Почему слой
устроен так — sql/ddl/45-dds-tables.sql; механика самой подмены —
sql/dds/order_replace.sql.
"""

from __future__ import annotations

import datetime
import logging
import os
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

# Окно изменяемости заказа, модельные дни: по нему трансформация решает, какие
# дни ещё могут измениться. Число принадлежит миру, владелец — генератор;
# почему у стенда лежит копия и чем она рискует — compose.yaml.
ORDER_WINDOW_DAYS = int(os.environ["ORDER_WINDOW_DAYS"])

# Позиция на оси модельного времени: номер первого несыгранного дня. Её ставит
# пульт мира, и только она отличает прожитый день от живого — зачем это
# сессиям, разобрано в sql/dds/session_scope.sql.
WORLD_POSITION = "world_position"


def _world_position() -> int:
    """Позиция мира числом, с внятным отказом на непригодном значении.

    Ту же переменную читает пульт, но общего модуля у дагов нет намеренно:
    связь между пультом и трансформацией держит сама переменная, а не импорт.
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
    # Потолок на задачи в прогоне. Размноженная замена норовит выстрелить
    # всеми днями сразу, а каждая задача — свой процесс исполнителя, и
    # планировщику отведено 640 МиБ (compose.yaml). Замерено: тринадцать дней
    # двух сущностей без потолка кончаются SIGKILL от контрольной группы.
    max_active_tasks=3,
    template_searchpath=str(SQL_ROOT),
    tags=["dds"],
)
def dds_transform():
    """Пересобрать сущности DDS по текущему состоянию источников."""

    @task_group(group_id="order")
    def order():
        """Заказ: дышащее окно плюс дни, которых в слое ещё нет."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у хранилища, какие дни собирает этот запуск."""
            query = (SQL_ROOT / "dds" / "order_scope.sql").read_text(encoding="utf-8")
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query, parameters={"window_days": ORDER_WINDOW_DAYS})
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

    order()
    session()


dds_transform()
