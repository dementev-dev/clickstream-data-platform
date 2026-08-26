"""Трансформации слоя DDS: пересборка дней, которые ещё могут измениться.

Даг собирает модель поверх принятого ODS. Первая его сущность — заказ; сессии
и карта идентичностей встанут соседними группами задач.

Расписания у дага нет и параметров тоже: объём каждого прогона он выводит сам
из состояния слоёв, поэтому один и тот же запуск одинаково верен из триггера
дневного конвейера, с кнопки менти и после падения
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
from airflow.sdk import dag, task, task_group

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

SQL_ROOT = Path("/opt/airflow/sql")
CLICKHOUSE_CONNECTION = "clickhouse_default"

# Окно изменяемости заказа, модельные дни: по нему трансформация решает, какие
# дни ещё могут измениться. Число принадлежит миру, владелец — генератор;
# почему у стенда лежит копия и чем она рискует — compose.yaml.
ORDER_WINDOW_DAYS = int(os.environ["ORDER_WINDOW_DAYS"])


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
            days = [
                (first_day + datetime.timedelta(days=shift)).isoformat()
                for shift in range((last_day - first_day).days + 1)
            ]
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

        @task
        def replacements(scope: dict[str, object]) -> list[dict[str, object]]:
            """Разложить дни в аргументы размноженной задачи замены.

            Шаг существует ради ограничения Airflow: размножать задачу можно
            только по целому возврату другой задачи — ни по ключу словаря, ни
            по карте поверх него. Отсюда отдельный возврат под один список.

            День едет в запрос подстановкой Jinja, а не параметром ClickHouse:
            в ALTER … PARTITION сервер параметр не подставляет — замерено,
            отвечает синтаксической ошибкой. Значение при этом не внешнее, его
            вернул сам ClickHouse запросом объёма.
            """
            return [{"params": {"day": day}} for day in scope["days"]]

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

    order()


dds_transform()
