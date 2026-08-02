"""План состава: чистота, приток и обещания, данные спекой (разделы 1, 5, 9).

Числа мира тесты сторожат вилками спеки, а не точными значениями: менти
крутит конфигурацию, и падать тесты должны там, где сдвинулся вывод («средний
магазин на 6–8 тыс. посетителей»), а не при каждой правке.
"""

import inspect
import re

import numpy as np
import pytest

from clickstream_generator import plan, world
from clickstream_generator.seeds import CANONICAL_SEED

# Горизонт эталонного снимка — две недели (спека, раздел 5).
SNAPSHOT_DAYS = 14


@pytest.fixture(autouse=True)
def fresh_memo():
    """Когорты запоминаются; тесты сравнивают вычисления, а не ссылки."""
    plan.cohort.cache_clear()


def visits_of(cohort: plan.Cohort) -> list[tuple[int, int]]:
    """Таблица визитов парами «кука — день», как её видит день-функция."""
    cookies, days = cohort.visit_cookie.tolist(), cohort.visit_day.tolist()
    return list(zip(cookies, days, strict=True))


def same_cohort(left: plan.Cohort, right: plan.Cohort) -> bool:
    return (
        left.day == right.day
        and left.people == right.people
        and np.array_equal(left.client_id, right.client_id)
        and np.array_equal(left.buyer, right.buyer)
        and np.array_equal(left.birth_day, right.birth_day)
        and np.array_equal(left.visit_cookie, right.visit_cookie)
        and np.array_equal(left.visit_day, right.visit_day)
        and np.array_equal(left.pair_cookies, right.pair_cookies)
        and np.array_equal(left.pair_order_days, right.pair_order_days)
    )


def test_the_plan_is_a_pure_function_of_the_seed():
    first = plan.cohort(CANONICAL_SEED, 3)
    plan.cohort.cache_clear()
    assert same_cohort(first, plan.cohort(CANONICAL_SEED, 3))


def test_counters_are_a_pure_function_of_the_seed():
    first = plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)
    plan.cohort.cache_clear()
    assert first == plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)


def test_a_remembered_cohort_cannot_be_spoiled_from_outside():
    """Когорту помнят и раздают всем дням окна: править её нельзя никак."""
    cohort = plan.cohort(CANONICAL_SEED, 0)
    with pytest.raises(ValueError):
        cohort.client_id[0] = 42
    with pytest.raises(ValueError):
        cohort.client_id.flags.writeable = True


def test_another_seed_is_another_world():
    ours = plan.cohort(CANONICAL_SEED, 3)
    theirs = plan.cohort(CANONICAL_SEED + 1, 3)
    assert not same_cohort(ours, theirs)


def test_a_day_costs_the_same_however_far_it_lies():
    """Горизонт не вход: день 500 стоит ровно столько же когорт, что день 5."""
    plan.audience(CANONICAL_SEED, 5)
    near = plan.cohort.cache_info().misses
    plan.cohort.cache_clear()
    plan.audience(CANONICAL_SEED, 500)
    assert plan.cohort.cache_info().misses == near


def test_the_world_has_a_prehistory_but_no_bottom_under_it():
    assert plan.cohort(CANONICAL_SEED, -world.RETURN_TAIL_DAYS).people > 0
    with pytest.raises(ValueError):
        plan.cohort(CANONICAL_SEED, -world.RETURN_TAIL_DAYS - 1)


def test_events_do_not_start_before_the_origin():
    with pytest.raises(ValueError):
        plan.audience(CANONICAL_SEED, -1)


@pytest.mark.parametrize("day", [-world.RETURN_TAIL_DAYS, -1, 0, 6, 13])
def test_visits_stay_inside_the_activity_window(day: int):
    """Окно активности — хвост возвратов от первого визита человека."""
    cohort = plan.cohort(CANONICAL_SEED, day)
    assert cohort.visit_day.min() == day
    assert cohort.visit_day.max() <= day + world.RETURN_TAIL_DAYS


def test_every_cookie_visits_on_the_day_it_was_born():
    cohort = plan.cohort(CANONICAL_SEED, 0)
    cookies = range(cohort.client_id.size)
    born = zip(cookies, cohort.birth_day.tolist(), strict=True)
    assert set(born) <= set(visits_of(cohort))


def test_a_cookie_visits_a_day_once():
    cohort = plan.cohort(CANONICAL_SEED, 0)
    visits = visits_of(cohort)
    assert visits == sorted(visits)
    assert len(set(visits)) == len(visits)


def test_client_ids_are_unique_and_survive_json():
    """Числа выше 2^53 в JSON округляются — куке столько не нужно."""
    cohort = plan.cohort(CANONICAL_SEED, 0)
    assert len(set(cohort.client_id.tolist())) == cohort.client_id.size
    assert cohort.client_id.max() < 2**53


def test_a_pair_is_one_buyer_with_two_cookies():
    cohort = plan.cohort(CANONICAL_SEED, 0)
    assert cohort.pairs > 0
    first, second = cohort.pair_cookies[:, 0], cohort.pair_cookies[:, 1]
    assert np.all(first < cohort.people)
    assert np.all(second >= cohort.people)
    assert np.all(cohort.buyer[cohort.pair_cookies])
    born_apart = cohort.birth_day[second] - cohort.birth_day[first]
    assert np.all(born_apart > 0)
    assert np.all(born_apart <= world.RETURN_TAIL_DAYS)
    # Вторая кука заводится, пока человек ещё ходит: обычно в первые дни.
    assert np.median(born_apart) < 14


def test_pairs_are_the_agreed_share_of_buyers():
    """15% покупателей (мастер-спека, раздел 5) — доля решена, не переоткрыта."""
    cohort = plan.cohort(CANONICAL_SEED, 0)
    buyers = int(cohort.buyer[: cohort.people].sum())
    assert 0.10 < cohort.pairs / buyers < 0.20
    assert 0.03 < buyers / cohort.people < 0.07


@pytest.mark.parametrize("day", [-world.RETURN_TAIL_DAYS, -20, 0, 5])
def test_every_pair_orders_from_both_cookies_on_the_axis(day: int):
    """Гарантия двухкуковых: заказ назначен на день визита куки, не раньше D0."""
    cohort = plan.cohort(CANONICAL_SEED, day)
    visits = set(visits_of(cohort))
    for cookies, days in zip(
        cohort.pair_cookies.tolist(), cohort.pair_order_days.tolist(), strict=True
    ):
        assert cookies[0] != cookies[1], "заказы пары — с двух разных кук"
        for cookie, order_day in zip(cookies, days, strict=True):
            assert order_day >= 0
            assert (cookie, order_day) in visits


def test_the_daily_audience_matches_the_spec_band():
    """6–8 тыс. посетителей в день — правдоподобный средний магазин."""
    counters = plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)
    assert all(6_000 <= size <= 8_000 for size in counters.audience)


def test_the_influx_holds_its_daily_number():
    counters = plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)
    average = sum(counters.new_cookies) / len(counters.new_cookies)
    assert abs(average - world.DAILY_INFLUX) < world.DAILY_INFLUX // 10


def test_the_influx_breathes_with_the_week():
    """Приток модулируется тем же недельным профилем, что трафик."""

    def mean(numbers: tuple[int, ...] | list[int]) -> float:
        return sum(numbers) / len(numbers)

    counters = plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)
    weekdays = [size for day, size in enumerate(counters.new_cookies) if day % 7 < 5]
    weekend = [size for day, size in enumerate(counters.new_cookies) if day % 7 >= 5]
    expected = mean(world.WEEKLY_PROFILE_PERCENT[5:]) / mean(
        world.WEEKLY_PROFILE_PERCENT[:5]
    )
    assert abs(mean(weekend) / mean(weekdays) - expected) < 0.03


def test_the_influx_differs_even_on_the_same_weekday():
    """Ровный приток выдал бы себя в первом же графике по дням."""
    mondays = {plan.cohort(CANONICAL_SEED, day).people for day in (0, 7, 14, 21)}
    assert len(mondays) > 1


def test_the_influx_is_visible_on_the_plan():
    """Накопленная аудитория растёт с горизонтом и с дневной не сходится."""
    week = plan.counters(CANONICAL_SEED, 7)
    fortnight = plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)
    assert week.visitors < fortnight.visitors
    assert fortnight.visitors > 2 * max(fortnight.audience)


def test_the_horizon_is_a_prefix_not_another_world():
    """Продление истории днём N+1 не трогает дни 1…N."""
    week = plan.counters(CANONICAL_SEED, 7)
    fortnight = plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)
    assert fortnight.audience[:7] == week.audience
    assert fortnight.new_cookies[:7] == week.new_cookies


def test_counters_count_the_same_audience_that_the_day_gets():
    """Счётчики и дня-функция спрашивают один план — расходиться им негде."""
    counters = plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)
    for day in (0, 7, SNAPSHOT_DAYS - 1):
        actual = plan.audience(CANONICAL_SEED, day).client_id.size
        assert actual == counters.audience[day]


def test_counters_know_the_pairs_before_a_single_event():
    counters = plan.counters(CANONICAL_SEED, SNAPSHOT_DAYS)
    assert counters.pairs > 0


def test_the_audience_of_a_day_carries_its_assigned_orders():
    audience = plan.audience(CANONICAL_SEED, 5)
    assert audience.client_id.size == audience.buyer.size
    assert audience.assigned_order.sum() > 0
    assert np.all(audience.buyer[audience.assigned_order])
    assert len(set(audience.client_id.tolist())) == audience.client_id.size


def test_randomness_is_drawn_in_whole_numbers():
    """Дисциплина спеки (раздел 2): целые числа, никакой системной математики.

    Сторож — разрешительный: плавающие распределения numpy расходятся между
    архитектурами и версиями (NEP 19), поэтому в план пускается только выбор
    целого.
    """
    for module in (plan, world):
        source = inspect.getsource(module)
        assert set(re.findall(r"\brng\.(\w+)", source)) <= {"integers"}
        assert not re.search(r"^\s*import (random|math)\b", source, re.MULTILINE)


def test_plan_arrays_are_whole_numbers():
    cohort = plan.cohort(CANONICAL_SEED, 0)
    for array in (
        cohort.client_id,
        cohort.birth_day,
        cohort.visit_cookie,
        cohort.visit_day,
        cohort.pair_cookies,
        cohort.pair_order_days,
    ):
        assert np.issubdtype(array.dtype, np.integer)
