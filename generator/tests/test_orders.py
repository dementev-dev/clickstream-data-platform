"""Заказы бэкенда: проекция покупки, деньги магазина и мост к склейке.

Здесь сверяются две проекции одной покупки — клиентское событие и заказ
бэкенда — и судьба заказа. Сверяется заказ не с внутренней структурой
торговой половины (это было бы сверкой кода с самим собой), а с событием,
которое уехало в трекер.

Чистота заказов от зерна и дня сторожится там же, где чистота событий, —
слепком дня в `test_day.py`: заказы день отдаёт наружу наравне с потоком.
"""

from typing import Any

import numpy as np
import pytest
from numpy.typing import NDArray

from clickstream_generator import catalog, commerce, day, orders, plan, schema, world
from clickstream_generator.inventory import STARTING_DAYS
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
def start_world() -> list[day.Day]:
    """Восемь дней, которыми `make up` наполняет пустой стенд."""
    return [day.stream(CANONICAL_SEED, number) for number in range(STARTING_DAYS)]


@pytest.fixture(scope="module")
def week(start_world: list[day.Day]) -> list[day.Day]:
    """Неделя мира: назначенные пары покупают в разные дни."""
    return start_world[:WEEK]


def purchases_of(events: day.Day) -> dict[str, NDArray[Any]]:
    """Строки покупок дня — колонками, в порядке потока."""
    here = events.columns["EventType"] == commerce.PURCHASE
    return {name: value[here] for name, value in events.columns.items()}


def user_id_by_order(events: day.Day) -> dict[str, int]:
    return dict(
        zip(events.orders.order_id, events.orders.user_id.tolist(), strict=True)
    )


def purchase_row_by_order(
    events: day.Day,
) -> tuple[dict[str, NDArray[Any]], dict[str, int]]:
    """Первая строка purchase каждого доехавшего заказа."""
    rows = purchases_of(events)
    index: dict[str, int] = {}
    for row, cell in enumerate(rows["purchaseID"]):
        index.setdefault(str(cell[0]), row)
    return rows, index


def test_every_order_of_the_day_is_a_purchase_of_the_day(weekday: day.Day):
    """Заказ рождается до потери или дубля его событийной проекции."""
    events = purchases_of(weekday)
    numbers = [cell[0] for cell in events["purchaseID"]]
    assert len(numbers) > 100
    lost = {
        order_id
        for order_id, outcome in zip(
            weekday.orders.order_id, weekday.purchase_outcome, strict=True
        )
        if outcome == commerce.EventOutcome.LOST
    }
    assert set(numbers) == set(weekday.orders.order_id) - lost
    assert len(numbers) - len(set(numbers)) == int(
        (weekday.purchase_outcome == commerce.EventOutcome.DUPLICATED).sum()
    )


def dropped_line(client: list[tuple[Any, int]], order: list[tuple[Any, int]]):
    """Единственный индекс, удалением которого корзина клиента даёт заказ."""
    for gone in range(len(client)):
        if client[:gone] + client[gone + 1 :] == order:
            return gone
    return None


def test_the_order_repeats_the_purchase_up_to_one_dropped_line(weekday: day.Day):
    """Заказ повторяет покупку или теряет ровно одну позицию с её деньгами.

    Дельта — единственное, чем заказ вправе разойтись с клиентом: строка
    уходит целиком, вместе со своей полной стоимостью, и терять её
    однопозиционному заказу нечего.
    """
    goods = catalog.catalog()
    price = dict(zip(goods.sku.tolist(), goods.price.tolist(), strict=True))
    events, row_by_order = purchase_row_by_order(weekday)
    money = weekday.orders
    deltas = 0

    for number, order in enumerate(weekday.orders.order_id):
        if weekday.purchase_outcome[number] == commerce.EventOutcome.LOST:
            continue
        row = row_by_order[order]
        client = list(
            zip(
                events["productID"][row].tolist(),
                events["productQuantity"][row].tolist(),
                strict=True,
            )
        )
        mine = list(
            zip(
                [goods.sku[item] for item in money.product[number].tolist()],
                money.quantity[number].tolist(),
                strict=True,
            )
        )
        # Выручка клиента и `items_total` бэкенда — одно число: в событии
        # оно дробное, у заказа целое в копейках.
        revenue = round(events["purchaseRevenue"][row][0] * commerce.KOPECKS)
        if mine == client:
            assert money.items_total[number] == revenue
            continue

        gone = dropped_line(client, mine)
        assert gone is not None, order
        assert len(client) > 1
        sku, quantity = client[gone]
        line = price[sku] * quantity
        assert money.items_total[number] == revenue - line
        deltas += 1

    # Сколько именно дельт — калибровка, а не контракт; ноль их быть не может.
    assert deltas > 0


def test_the_discount_comes_from_the_coupon_of_the_event(
    start_world: list[day.Day],
):
    """Промокод даёт скидку на корзину, которую склад оставил в заказе."""
    percent = dict(world.COUPONS)
    discounted_delta = 0

    for today in start_world:
        money = today.orders
        events, row_by_order = purchase_row_by_order(today)
        for number, order_id in enumerate(money.order_id):
            if today.purchase_outcome[number] == commerce.EventOutcome.LOST:
                continue
            row = row_by_order[order_id]
            code = events["purchaseCoupon"][row][0]
            items_total = money.items_total[number]
            expected = items_total * percent[code] // 100 if code else 0
            assert money.discount[number] == expected, money.order_id[number]
            assert money.discount[number] < items_total

            revenue = round(events["purchaseRevenue"][row][0] * commerce.KOPECKS)
            discounted_delta += int(bool(code) and items_total != revenue)

    # Иначе проверка не отличила бы новую базу скидки от прежней.
    assert discounted_delta > 0


def test_the_money_of_start_world_orders_adds_up(start_world: list[day.Day]):
    """Деньги всех восьми стартовых дней целые, связные и неотрицательные."""
    deliveries: set[int] = set()

    for today in start_world:
        money = today.orders
        assert np.array_equal(
            money.total, money.items_total - money.discount + money.delivery
        )
        for name, column in (
            ("items_total", money.items_total),
            ("discount", money.discount),
            ("delivery", money.delivery),
            ("total", money.total),
        ):
            assert np.issubdtype(column.dtype, np.integer)
            negative = np.flatnonzero(column < 0).tolist()
            assert not negative, (
                today.day,
                name,
                [money.order_id[number] for number in negative],
            )

        deliveries.update(money.delivery.tolist())

    prices = {price for price, _ in world.DELIVERY_KOPECKS_WEIGHTS}
    # Доставка — деньги, которых нет ни в одном событии: без неё «считаем по
    # бэкенду» ничего не значило бы.
    assert deliveries == prices


def test_every_order_leaves_the_window_with_one_of_three_fates(weekday: day.Day):
    """Судьба заказа: три исхода, и моменты рассказывают ту же историю.

    Инвариант выхода из окна — заказ либо оплачен, либо отменён; `created`
    навсегда мир не допускает. Момента, которого у исхода нет, нет и в
    данных: его место занимает −1, а не ноль, иначе «оплатили в секунду
    рождения» было бы не отличить от «не оплатили вовсе». Оставшиеся
    моменты лежат внутри окна 144 часов — таблица весов кончается там же,
    где окно у самого невезучего заказа, — а секунда внутри часа
    равномерная: без неё разности времён аудита давали бы точные равенства.

    Доли здесь не спрашиваются: их калибруют, а не фиксируют тестом.
    """
    window = 144 * 3600
    fate = weekday.orders
    outcomes = fate.outcome.tolist()
    assert len(outcomes) == len(fate)
    assert set(outcomes) == {
        orders.OrderOutcome.PAID,
        orders.OrderOutcome.PAID_THEN_CANCELLED,
        orders.OrderOutcome.UNPAID_THEN_CANCELLED,
    }

    for outcome, paid, cancelled in zip(
        outcomes, fate.paid_after.tolist(), fate.cancelled_after.tolist(), strict=True
    ):
        for moment in (paid, cancelled):
            assert moment == -1 or 0 <= moment < window
        if outcome == orders.OrderOutcome.PAID:
            assert paid >= 0 and cancelled == -1
        elif outcome == orders.OrderOutcome.PAID_THEN_CANCELLED:
            # Ранний из двух моментов и есть оплата: порядок дорог выходит
            # сортировкой, а не условной точкой отсчёта.
            assert 0 <= paid <= cancelled
        else:
            assert paid == -1 and cancelled >= 0

    seconds = {
        moment % 3600
        for moment in (*fate.paid_after.tolist(), *fate.cancelled_after.tolist())
        if moment >= 0
    }
    assert len(seconds) > 1


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
    by_order = user_id_by_order(weekday)
    for order_id, cookie in zip(
        (cell[0] for cell in events["purchaseID"]),
        events["ClientID"].tolist(),
        strict=True,
    ):
        assert by_order[order_id] == person[cookie]
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

    Спрашивается двумя способами, потому что утечь личность может двумя.
    Числом — тогда её видно в числовых колонках дня, и они сверяются со
    всеми личностями дневной аудитории. Текстом — тогда сравнение чисел
    её прозевало бы, поэтому строки покупок читаются целиком, вместе с
    сырым `ecommerce`: этот блок собирается руками, и дописать в него
    лишнее поле проще всего. Имени для личности в контракте схемы нет.
    """
    people = set(plan.audience(CANONICAL_SEED, WEEKDAY).person_id.tolist())
    assert people
    names = {column.name.lower() for column in schema.COLUMNS}
    assert not (names & {"userid", "user_id", "personid", "person_id"})
    for name, value in weekday.columns.items():
        if np.issubdtype(value.dtype, np.integer):
            assert not (set(value.tolist()) & people), name

    events = purchases_of(weekday)
    text = "\n".join(
        " ".join(str(value[row]) for value in events.values())
        for row in range(events["WatchID"].size)
    )
    for person in weekday.orders.user_id.tolist():
        assert str(person) not in text
    assert set(weekday.orders.user_id.tolist()) <= people
