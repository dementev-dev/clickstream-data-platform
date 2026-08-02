"""Сторожа конфигурации мира: связность чисел, решённых спекой (раздел 9).

Крутить числа менти можно и нужно — тесты сторожат не значения, а то, на чём
держатся выводы спеки: D0 — понедельник (от него считается день недели),
недельный профиль в среднем даёт единицу, профиль возвратов сходится с
«в среднем 3–4 возврата» и «средняя кука активна ≈1,9 дня».
"""

from clickstream_generator import world


def mean_by_weights(values: tuple[int, ...], weights: tuple[int, ...]) -> float:
    pairs = zip(values, weights, strict=True)
    weighted = sum(value * weight for value, weight in pairs)
    return weighted / sum(weights)


def test_origin_is_a_monday():
    """День недели считается как остаток номера дня — это верно от понедельника."""
    assert world.ORIGIN.weekday() == 0


def test_weekly_profile_covers_a_week_and_averages_to_one():
    assert len(world.WEEKLY_PROFILE_PERCENT) == 7
    assert sum(world.WEEKLY_PROFILE_PERCENT) == 700


def test_returning_share_matches_the_spec():
    assert world.ONE_SHOT_PERCENT == 75


def test_returns_average_three_to_four():
    counts = tuple(range(1, len(world.RETURN_COUNT_WEIGHTS) + 1))
    assert 3 <= mean_by_weights(counts, world.RETURN_COUNT_WEIGHTS) <= 4


def test_average_cookie_is_active_about_two_days():
    """Сходимость с разделом 5: ≈1,9 активного дня на куку — отсюда 6–8 тыс."""
    counts = tuple(range(1, len(world.RETURN_COUNT_WEIGHTS) + 1))
    returns = mean_by_weights(counts, world.RETURN_COUNT_WEIGHTS)
    active_days = 1 + (100 - world.ONE_SHOT_PERCENT) / 100 * returns
    assert 1.8 <= active_days <= 2.0


def test_return_delays_cover_the_whole_activity_window():
    delays = world.RETURN_DELAY_WEIGHTS
    assert len(delays) == world.RETURN_TAIL_DAYS
    assert all(weight > 0 for weight in delays)


def test_return_profile_decays_towards_the_edge():
    """Затухание — чтобы обрыв на краю окна в данных был не виден."""
    delays = world.RETURN_DELAY_WEIGHTS
    steps = zip(delays, delays[1:], strict=False)
    assert all(later <= earlier for earlier, later in steps)
    assert delays[-1] * 10 < delays[0]


def test_most_returns_land_in_the_first_days():
    delays = world.RETURN_DELAY_WEIGHTS
    assert sum(delays[:10]) > sum(delays[10:])


def test_pairs_are_a_small_part_of_the_cohort():
    """Вторые куки пар добавляют к притоку меньше процента (спека, раздел 9)."""
    assert world.BUYER_PERCENT * world.PAIRED_BUYER_PERCENT < 100
