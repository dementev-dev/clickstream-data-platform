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


def test_both_daily_waves_cover_a_day_and_average_to_one():
    """Форма волны не меняет суточный объём: его задаёт недельный профиль."""
    for profile in (world.WEEKDAY_HOURS_PERCENT, world.WEEKEND_HOURS_PERCENT):
        assert len(profile) == 24
        assert sum(profile) == 2400


def test_the_wave_dips_at_night_and_peaks_up_to_twice_the_average():
    """Пики до ~2× среднего, ночью провал (спека, раздел 2)."""
    for profile in (world.WEEKDAY_HOURS_PERCENT, world.WEEKEND_HOURS_PERCENT):
        assert min(profile[2:6]) < 50
        assert 150 < max(profile) <= 220


def test_the_weekend_wakes_up_later_than_a_weekday():
    morning = slice(7, 10)
    assert sum(world.WEEKEND_HOURS_PERCENT[morning]) < sum(
        world.WEEKDAY_HOURS_PERCENT[morning]
    )


def test_an_active_day_holds_a_visit_or_two():
    """Дневная аудитория 6–8 тыс. и 8–12 тыс. визитов сходятся через это число."""
    weights = world.VISITS_PER_ACTIVE_DAY_WEIGHTS
    counts = tuple(range(1, len(weights) + 1))
    assert 1.2 <= mean_by_weights(counts, weights) <= 1.6


def test_a_visit_is_a_few_pages_long_and_often_a_single_one():
    weights = world.VISIT_PAGES_WEIGHTS
    pages = tuple(range(1, len(weights) + 1))
    assert 4 <= mean_by_weights(pages, weights) <= 6
    bounced = weights[0] / sum(weights)
    assert 0.15 < bounced < 0.35


def test_pauses_stay_inside_the_visit_timeout():
    """Иначе визит распался бы там, где генератор этого не обещал."""
    assert max(world.PAGE_PAUSE_SECONDS) < world.VISIT_TIMEOUT_SECONDS
    assert max(world.LONG_PAUSE_SECONDS) < world.VISIT_TIMEOUT_SECONDS


def test_the_funnel_converts_about_two_percent_of_visits():
    """Конверсия ~2% на сессию (спека, раздел 9) — произведение трёх шагов."""
    conversion = (
        world.CART_PERCENT
        * world.CHECKOUT_OF_CART_PERCENT
        * world.CONFIRMATION_OF_CHECKOUT_PERCENT
        / 100**2
    )
    assert 1.5 <= conversion <= 2.5
