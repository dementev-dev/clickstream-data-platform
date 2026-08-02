"""Сторожа конфигурации мира: связность чисел, решённых спекой (раздел 9).

Крутить числа менти можно и нужно — тесты сторожат не значения, а то, на чём
держатся выводы спеки: D0 — понедельник (от него считается день недели),
недельный профиль в среднем даёт единицу, профиль возвратов сходится с
«в среднем 3–4 возврата» и «средняя кука активна ≈1,9 дня».
"""

from clickstream_generator import catalog, world


def mean_by_weights(values: tuple[int, ...], weights: tuple[int, ...]) -> float:
    pairs = zip(values, weights, strict=True)
    weighted = sum(value * weight for value, weight in pairs)
    return weighted / sum(weights)


def active_days(one_shot_percent: int, return_weights: tuple[int, ...]) -> float:
    """Сколько дней в среднем ходит кука: день рождения плюс возвраты."""
    counts = tuple(range(1, len(return_weights) + 1))
    returns = mean_by_weights(counts, return_weights)
    return 1 + (100 - one_shot_percent) / 100 * returns


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
    assert 1.8 <= active_days(world.ONE_SHOT_PERCENT, world.RETURN_COUNT_WEIGHTS) <= 2.0


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


def test_the_trade_event_never_overtakes_the_next_page():
    """Торговое событие отстаёт от своей страницы меньше, чем длится пауза.

    На этом стоят два свойства сразу: событие корзины не обгоняет страницу,
    на которой посетитель нажал кнопку, и не выходит за полночь — за ним в
    том же визите всегда идёт страница, которая полночь пережила.
    """
    assert max(world.TRADE_DELAY_SECONDS) <= min(world.PAGE_PAUSE_SECONDS)


def conversion(cart: int, checkout_of_cart: int) -> float:
    """Конверсия визита в заказ, проценты: произведение трёх шагов воронки."""
    return cart * checkout_of_cart * world.CONFIRMATION_OF_CHECKOUT_PERCENT / 100**2


def test_the_ordinary_visitor_converts_a_bit_below_the_world():
    """Воронка обычного посетителя: ~1,5% визитов в оформленный заказ.

    До ~2% конверсию мира (спека, раздел 9) добирают помеченные покупатели
    и обещанные планом заказы пар, поэтому здесь сторожится слагаемое, а не
    итог: сам итог меряет день-функция на собранном дне
    (`test_the_shop_stays_the_same_plausible_shop`).
    """
    assert 1.2 <= conversion(world.CART_PERCENT, world.CHECKOUT_OF_CART_PERCENT) <= 1.8


def test_the_buyer_mark_shows_on_both_steps_of_the_funnel():
    """Один шаг дал бы половину картины: кладут реже и бросают чаще оба."""
    assert world.BUYER_CART_PERCENT > world.CART_PERCENT
    assert world.BUYER_CHECKOUT_OF_CART_PERCENT > world.CHECKOUT_OF_CART_PERCENT
    lift = conversion(
        world.BUYER_CART_PERCENT, world.BUYER_CHECKOUT_OF_CART_PERCENT
    ) / conversion(world.CART_PERCENT, world.CHECKOUT_OF_CART_PERCENT)
    # Заметно чаще прочих, но не «покупают только помеченные»: 5% людей дали
    # бы тогда около 40 заказов в день вместо 240 (спека, раздел 9).
    assert 2 <= lift <= 6


def test_the_demand_levels_keep_their_order_and_yield_to_the_intent():
    """Ряды вероятностей читаются по уровням каталога, и оба убывают.

    Совпадение длин — не формальность: ряды индексируются номером уровня,
    и новый уровень в файле каталога обязан получить здесь своё число.
    """
    rows = (world.SHOPPING_ADD_PERCENT, world.BROWSING_ADD_PERCENT)
    for row in rows:
        assert len(row) == len(catalog.DEMAND_LEVELS)
        assert row[0] > row[1] > row[2], row
    # Намерение сильнее товара: визит, пришедший покупать, кладёт чаще на
    # любом уровне — иначе слово «намерение» ничего бы не значило.
    assert all(shopping > browsing for shopping, browsing in zip(*rows, strict=True))
    # Но и не всё подряд: товар решает у обоих, спрос не декорация.
    assert min(world.SHOPPING_ADD_PERCENT) < 100


def test_the_buyer_mark_makes_the_cookie_live_longer():
    """Второй рычаг метки: без долгой жизни второй покупке негде случиться."""
    assert world.BUYER_ONE_SHOT_PERCENT < world.ONE_SHOT_PERCENT
    usual = active_days(world.ONE_SHOT_PERCENT, world.RETURN_COUNT_WEIGHTS)
    buyer = active_days(world.BUYER_ONE_SHOT_PERCENT, world.BUYER_RETURN_COUNT_WEIGHTS)
    assert buyer > 2 * usual
    # Но не бессмертие: дневная аудитория остаётся в вилке 6–8 тыс., а
    # помеченных всего 5% людей когорты.
    assert buyer < world.RETURN_TAIL_DAYS / 10
