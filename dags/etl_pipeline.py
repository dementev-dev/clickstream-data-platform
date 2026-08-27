"""Дневной конвейер: DDS, DM и проверка качества.

Своих преобразований у дага нет. Ждущие триггеры связывают состояния:
если слой краснеет, конвейер не запускает следующий слой и краснеет сам.
"""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)


@dag(
    dag_id="etl_pipeline",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["etl"],
)
def etl_pipeline():
    """Собрать слои последовательно по текущему состоянию данных."""
    dds = TriggerDagRunOperator(
        task_id="trigger_dds",
        trigger_dag_id="dds_transform",
        wait_for_completion=True,
        poke_interval=10,
    )
    dm = TriggerDagRunOperator(
        task_id="trigger_dm",
        trigger_dag_id="dm_transform",
        wait_for_completion=True,
        poke_interval=10,
    )
    # Airflow 3.3: оператор провайдера standard с wait_for_completion=True
    # поднимает AirflowException, если запущенный даг завершился красным.
    # Поведение и путь импорта повторно сверены через Context7 27 августа
    # 2026 года.
    dq = TriggerDagRunOperator(
        task_id="trigger_dq",
        trigger_dag_id="dq_check",
        wait_for_completion=True,
        poke_interval=10,
    )

    dds >> dm >> dq


etl_pipeline()
