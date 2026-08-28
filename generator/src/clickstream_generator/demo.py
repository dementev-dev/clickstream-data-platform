"""Короткий проход по цепочке генератора на каноническом дне D0."""

from collections import Counter

import numpy as np

from clickstream_generator import day, plan, seeds, world

DAY = 0


def _number(value: int) -> str:
    """Разделяет разряды числа для чтения с экрана."""
    return f"{value:,}".replace(",", " ")


def render() -> str:
    """Собирает D0 и показывает его стадии без числового эталона."""
    seed = seeds.CANONICAL_SEED
    cohort = plan.cohort(seed, DAY)
    audience = plan.audience(seed, DAY)
    events = day.stream(seed, DAY)

    _, rows_per_visit = np.unique(events.columns["VisitID"], return_counts=True)
    event_types = Counter(str(value) for value in events.columns["EventType"])
    smallest_visit = int(rows_per_visit.min())
    largest_visit = int(rows_per_visit.max())

    return "\n".join(
        (
            f"Зерно: {seed}",
            "Подпотоки D0: состав мира → когорта; день → трафик, торговля, заказы",
            (
                f"Когорта D0: людей — {_number(cohort.people)}; "
                f"кук — {_number(len(cohort.client_id))}"
            ),
            (
                f"Аудитория D0: кук — {_number(len(audience.client_id))}; "
                f"когорты D-{world.RETURN_TAIL_DAYS}...D0"
            ),
            (
                f"Визиты по VisitID: групп — {_number(len(rows_per_visit))}; "
                f"строк в группе — {smallest_visit}–{largest_visit}"
            ),
            (
                f"Строки событий: {_number(len(events))} "
                f"(pageview — {_number(event_types['pageview'])}; "
                f"add_to_cart — {_number(event_types['add_to_cart'])}; "
                f"purchase — {_number(event_types['purchase'])}); "
                f"заказов — {_number(len(events.orders))}"
            ),
        )
    )


def main() -> None:
    """Печатает проход по D0."""
    print(render())


if __name__ == "__main__":
    main()
