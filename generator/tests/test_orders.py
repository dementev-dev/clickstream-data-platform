"""Заказы бэкенда: проекция покупки, деньги магазина и мост к склейке.

Заказ здесь ещё без судьбы — статуса, моментов и дельты (тикет #91): всё,
что проверяется, это согласие двух источников по построению. Поэтому и
сверяется заказ не с внутренней структурой торговой половины (это было бы
сверкой кода с самим собой), а с событием, которое уехало в трекер.

Чистота заказов от зерна и дня сторожится там же, где чистота событий, —
слепком дня в `test_day.py`: заказы день отдаёт наружу наравне с потоком.
"""

from typing import Any

import numpy as np
import pytest
from numpy.typing import NDArray

from clickstream_generator import catalog, commerce, day, orders, plan, schema, world
from clickstream_generator.seeds import CANONICAL_SEED, Component

WEEKDAY = 2
WEEK = 7


@pytest.fixture(autouse=True)
def fresh_memo():
    """Когорты запоминаются; тесты сравнивают вычисления, а не ссылки."""
    plan.cohort.cache_clear()


@pytest.fixture(scope="module")
def weekday() -> day.Day:
    return day.stream(CANONICAL_SEED, WEEKDAY)


@pytest.fixture(scope="module")
def week() -> list[day.Day]:
    """Неделя мира: назначенные пары покупают в разные дни."""
    return [day.stream(CANONICAL_SEED, number) for number in range(WEEK)]


def purchases_of(events: day.Day) -> dict[str, NDArray[Any]]:
    """Строки покупок дня — колонками, в порядке потока."""
    here = events.columns["EventType"] == commerce.PURCHASE
    return {name: value[here] for name, value in events.columns.items()}


def user_id_by_order(events: day.Day) -> dict[str, int]:
    return dict(
        zip(events.orders.order_id, events.orders.user_id.tolist(), strict=True)
    )


def test_every_order_of_the_day_is_a_purchase_of_the_day(weekday: day.Day):
    """Заказ — вторая проекция покупки, а не второе порождение.

    У всякого заказа ровно одно событие `purchase`, и номер у них один:
    сойтись двум источникам больше негде — сверка стенда соединяется
    именно по нему.
    """
    events = purchases_of(weekday)
    numbers = [cell[0] for cell in events["purchaseID"]]
    assert len(numbers) > 100
    assert list(weekday.orders.order_id) == numbers


def test_the_order_repeats_the_basket_and_the_money_of_its_purchase(weekday: day.Day):
    """Корзина и деньги клиента у заказа те же — он их не пересчитывает."""
    goods = catalog.catalog()
    events = purchases_of(weekday)
    for number, order in enumerate(weekday.orders.order_id):
        assert order == events["purchaseID"][number][0]
        items = weekday.orders.product[number]
        assert [goods.sku[item] for item in items.tolist()] == (
            events["productID"][number].tolist()
        )
        assert weekday.orders.quantity[number].tolist() == (
            events["productQuantity"][number].tolist()
        )
        # Выручка клиента и `items_total` бэкенда — одно число: в событии
        # оно дробное, у заказа целое в копейках.
        assert weekday.orders.items_total[number] == round(
            events["purchaseRevenue"][number][0] * commerce.KOPECKS
        )


def test_the_discount_comes_from_the_coupon_of_the_event(weekday: day.Day):
    """Промокод в событии обязан обернуться скидкой — иначе данные соврут."""
    percent = dict(world.COUPONS)
    events = purchases_of(weekday)
    codes = [cell[0] for cell in events["purchaseCoupon"]]
    assert sum(1 for code in codes if code) > 10

    for number, code in enumerate(codes):
        total = int(weekday.orders.items_total[number])
        expected = total * percent[code] // 100 if code else 0
        assert weekday.orders.discount[number] == expected
    # Скидка без кода не берётся ниоткуда, а с кодом не съедает заказ.
    assert np.all(weekday.orders.discount < weekday.orders.items_total)


def test_the_money_of_an_order_adds_up(weekday: day.Day):
    """`total` = `items_total` − `discount` + `delivery`, целыми копейками."""
    money = weekday.orders
    assert np.array_equal(
        money.total, money.items_total - money.discount + money.delivery
    )
    for column in (money.items_total, money.discount, money.delivery, money.total):
        assert np.issubdtype(column.dtype, np.integer)
        assert np.all(column >= 0)

    prices = {price for price, _ in world.DELIVERY_KOPECKS_WEIGHTS}
    assert set(money.delivery.tolist()) == prices
    # Доставка — деньги, которых нет ни в одном событии: без неё «считаем по
    # бэкенду» ничего не значило бы.
    assert np.any(money.total != money.items_total - money.discount)


def test_the_order_side_draws_from_its_own_named_branch(
    monkeypatch: pytest.MonkeyPatch,
):
    """Заказная сторона берёт свой подпоток и ветвится по дню рождения заказа.

    Адрес в дереве зерна и есть контракт компонента (спека генератора,
    раздел 2): правка заказной механики не двигает ни трафик, ни торговлю,
    а слепок читает судьбу заказа из дня, в который заказ родился.
    """
    asked: list[tuple[int, int, Component]] = []
    honest = orders.day_stream

    def spy(seed: int, day_number: int, component: Component):
        asked.append((seed, day_number, component))
        return honest(seed, day_number, component)

    monkeypatch.setattr(orders, "day_stream", spy)
    day.stream(CANONICAL_SEED, WEEKDAY)
    assert asked == [(CANONICAL_SEED, WEEKDAY, Component.ORDERS)]


def test_the_user_id_is_the_person_behind_the_cookie(weekday: day.Day):
    """Личность заказу даёт план, а не разбор трекера."""
    audience = plan.audience(CANONICAL_SEED, WEEKDAY)
    person = dict(
        zip(audience.client_id.tolist(), audience.person_id.tolist(), strict=True)
    )
    events = purchases_of(weekday)
    for number, cookie in enumerate(events["ClientID"].tolist()):
        assert weekday.orders.user_id[number] == person[cookie]
    assert np.all(weekday.orders.user_id > 0)
    assert weekday.orders.user_id.max() < 2**53


def test_both_cookies_of_a_pair_order_as_one_user(week: list[day.Day]):
    """Мост к склейке: заказы с двух кук пары несут один `user_id`.

    Проверяются пары, у которых оба назначенных дня попали внутрь недели:
    остальные ждут своего дня за горизонтом.
    """
    users: list[dict[int, set[int]]] = []
    for events in week:
        by_order = user_id_by_order(events)
        rows = purchases_of(events)
        seen: dict[int, set[int]] = {}
        for cookie, number in zip(
            rows["ClientID"].tolist(),
            (cell[0] for cell in rows["purchaseID"]),
            strict=True,
        ):
            seen.setdefault(cookie, set()).add(by_order[number])
        users.append(seen)

    pairs = 0
    for born in range(-world.RETURN_TAIL_DAYS, WEEK):
        cohort = plan.cohort(CANONICAL_SEED, born)
        for cookies, days in zip(
            cohort.pair_cookies.tolist(), cohort.pair_order_days.tolist(), strict=True
        ):
            if max(days) >= WEEK:
                continue
            pairs += 1
            first, second = (
                users[day_number][int(cohort.client_id[cookie])]
                for cookie, day_number in zip(cookies, days, strict=True)
            )
            assert len(first | second) == 1
    assert pairs > 10


def test_the_user_id_never_reaches_the_metrica_event(weekday: day.Day):
    """Кликстрим анонимен: личность держится формой контракта, а не забывчивостью.

    Проверяются числовые колонки события целиком: личность — число того же
    порядка, что кука и номер события, и попасть она могла бы только в
    такую. Имени для неё в контракте нет вовсе.
    """
    people = set(plan.audience(CANONICAL_SEED, WEEKDAY).person_id.tolist())
    assert people
    names = {column.name.lower() for column in schema.COLUMNS}
    assert not (names & {"userid", "user_id", "personid", "person_id"})
    for name, value in weekday.columns.items():
        if np.issubdtype(value.dtype, np.integer):
            assert not (set(value.tolist()) & people), name
    assert set(weekday.orders.user_id.tolist()) <= people
