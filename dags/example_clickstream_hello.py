"""Пример независимого DAG для проверки Airflow 3."""

from __future__ import annotations

import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="example_clickstream_hello",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["пример"],
    doc_md=__doc__,
)
def example_clickstream_hello():
    @task
    def extract() -> dict[str, int]:
        return {"clicks": 3, "views": 10}

    @task
    def calculate_ctr(counters: dict[str, int]) -> float:
        return round(counters["clicks"] / counters["views"], 3)

    @task
    def show_result(ctr: float) -> None:
        print(f"CTR учебного примера: {ctr}")

    show_result(calculate_ctr(extract()))


example_clickstream_hello()
