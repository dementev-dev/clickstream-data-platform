"""Торговые события: корзина шире заказа, деньги целые, номера читаемые.

Числа мира тесты сторожат вилками спеки, а не точными значениями: менти
крутит конфигурацию, и падать тесты должны там, где сдвинулся вывод («заказ
уже корзины», «конверсия визита около 2%»), а не при каждой правке.

Неделя дня-функции стоит секунд, поэтому дни, которые нужны нескольким
тестам, считаются один раз на модуль.
"""

import json
from typing import Any

import numpy as np
import pytest
from numpy.typing import NDArray

from clickstream_generator import catalog, commerce, day, plan, schema, world
from clickstream_generator.reference import Page
from clickstream_generator.seeds import CANONICAL_SEED, Component

WEEKDAY = 2
WEEK = 7
# Окно эталонного снимка: доли по уровням спроса меряются на нём целиком —
# на одном дне у самого редкого уровня набирается слишком мало карточек.
FORTNIGHT = 14


@pytest.fixture(autouse=True)
def fresh_memo():
    """Когорты запоминаются; тесты сравнивают вычисления, а не ссылки."""
    plan.cohort.cache_clear()


@pytest.fixture(scope="module")
def weekday() -> day.Day:
    return day.stream(CANONICAL_SEED, WEEKDAY)


@pytest.fixture(scope="module")
def week() -> list[day.Day]:
    """Неделя мира: повторные покупки и обещания пар живут между днями."""
    return [day.stream(CANONICAL_SEED, number) for number in range(WEEK)]


@pytest.fixture(scope="module")
def fortnight(week: list[day.Day]) -> list[day.Day]:
    """Две недели: неделя пересчитанной не бывает — день чист от соседей."""
    later = range(WEEK, FORTNIGHT)
    return week + [day.stream(CANONICAL_SEED, number) for number in later]


def rows_of(events: day.Day, kind: str) -> dict[str, NDArray[Any]]:
    """Строки одного типа события — все колонки разом."""
    here = events.columns["EventType"] == kind
    return {name: value[here] for name, value in events.columns.items()}


def cells(column: NDArray[Any]) -> list[list[Any]]:
    return [cell.tolist() for cell in column]


def raw_of(rows: dict[str, NDArray[Any]]) -> list[dict[str, Any]]:
    """Сырой `ecommerce` разобранный — чужим разбором, не своим сборщиком."""
    return [json.loads(text) for text in rows["ecommerce"]]


def by_visit(events: day.Day) -> NDArray[np.int64]:
    """Порядок строк «визит, потом время» — так их читает лаба сессий."""
    return np.lexsort((events.columns["UTCEventTime"], events.columns["VisitID"]))


def test_the_day_carries_both_trade_events(weekday: day.Day):
    """Таксономия ожила: в дне есть и корзина, и покупка."""
    carts = rows_of(weekday, commerce.ADD_TO_CART)
    orders = rows_of(weekday, commerce.PURCHASE)
    assert orders["WatchID"].size > 100
    # Корзина шире заказа и по событиям: не всякая корзина доходит до кассы.
    assert carts["WatchID"].size > 2 * orders["WatchID"].size


def visits_with(events: day.Day, page: int) -> set[int]:
    """Визиты, у которых эта страница магазина дожила до потока.

    Считаются просмотры страниц, а не строки с такой же страницей: торговое
    событие садится на свою страницу и несёт её же, поэтому маска по одному
    `page` посчитала бы событие корзины за просмотр карточки.
    """
    here = (events.columns["EventType"] == "pageview") & (events.page == page)
    return set(events.columns["VisitID"][here].tolist())


def confirmed_visits(events: day.Day) -> set[int]:
    """Визиты, чья страница подтверждения дожила до потока."""
    return visits_with(events, Page.CONFIRMATION)


def visits_that_put(events: day.Day) -> set[int]:
    """Визиты с событием корзины — по типу события, а не по массивам товаров.

    Строка покупки тоже несёт товары, поэтому «строка с `productID`» — это
    не «событие корзины».
    """
    return set(rows_of(events, commerce.ADD_TO_CART)["VisitID"].tolist())


def cart_products(events: day.Day) -> list[int]:
    """Номера товаров, положенных в корзину, — по позиции на строку."""
    here = events.columns["EventType"] == commerce.ADD_TO_CART
    return events.product[here].tolist()


def card_views(events: day.Day) -> int:
    """Просмотры карточек товара в дне — строками потока, с повторами."""
    pageview = events.columns["EventType"] == "pageview"
    return int((pageview & (events.page == Page.PRODUCT)).sum())


def opened_cards(events: day.Day) -> set[tuple[int, int]]:
    """Разные карточки визитов: повторный просмотр внутри визита — один раз.

    Так же считается и позиция корзины, поэтому это и есть честный
    знаменатель конверсии «просмотр карточки → корзина».
    """
    pageview = events.columns["EventType"] == "pageview"
    here = pageview & (events.page == Page.PRODUCT)
    return set(
        zip(
            events.columns["VisitID"][here].tolist(),
            events.product[here].tolist(),
            strict=True,
        )
    )


def test_a_confirmation_in_the_stream_always_has_its_purchase(weekday: day.Day):
    """Клиентская сторона честная: подтверждение без покупки — это потеря.

    Потери и дубли стенд заводит намеренно и позже (этапы 4 и 6); здесь их
    быть не должно ни одной. Обратное тоже верно: покупка без подтверждения
    в потоке — заказ ниоткуда.
    """
    bought = set(rows_of(weekday, commerce.PURCHASE)["VisitID"].tolist())
    assert confirmed_visits(weekday) == bought


def test_the_day_boundary_takes_the_confirmation_together_with_its_purchase(
    monkeypatch: pytest.MonkeyPatch,
):
    """Полночь режет подтверждение и покупку вместе — или не режет ни одну.

    Иначе визит отдал бы страницу «Заказ оформлен» без события покупки, и на
    клиентской стороне завелась бы потеря. Хвост торгового события здесь
    растянут до часа: в каноническом мире он секунды, и такой визит
    выпадает раз в годы, а правило обязано держаться при любом хвосте.
    """
    monkeypatch.setattr(world, "TRADE_DELAY_SECONDS", (3_000, 3_600))
    monkeypatch.setattr(day, "TRADE_TAIL_SECONDS", 3_600)
    events = day.stream(CANONICAL_SEED, WEEKDAY)
    bought = set(rows_of(events, commerce.PURCHASE)["VisitID"].tolist())
    assert bought
    assert confirmed_visits(events) == bought


def test_a_purchase_carries_exactly_one_order_number(weekday: day.Day):
    """Одно подтверждение — один заказ (мастер-спека, раздел 4)."""
    orders = rows_of(weekday, commerce.PURCHASE)
    for name in ("purchaseID", "purchaseRevenue", "purchaseCurrency", "purchaseCoupon"):
        assert {cell.size for cell in orders[name]} == {1}, name
    assert {cell[0] for cell in orders["purchaseCurrency"]} == {world.CURRENCY}


def test_the_cart_event_has_no_order_block(weekday: day.Day):
    """У корзины заказа ещё нет: блок `purchase*` пуст, как у просмотра."""
    carts = rows_of(weekday, commerce.ADD_TO_CART)
    for name in ("purchaseID", "purchaseRevenue", "purchaseCurrency", "purchaseCoupon"):
        assert {cell.size for cell in carts[name]} == {0}, name


def test_the_product_arrays_of_an_event_share_one_length(weekday: day.Day):
    """Длина общая внутри группы `product*`; с `purchase*` она не совпадает."""
    trade = weekday.columns["EventType"] != "pageview"
    names = [
        column.name for column in schema.COLUMNS if column.name.startswith("product")
    ]
    assert len(names) == 6
    sizes = np.array(
        [[cell.size for cell in weekday.columns[name][trade]] for name in names]
    )
    assert np.all(sizes == sizes[0])
    # Корзина — всегда один товар, заказ — от одного и больше.
    carts = rows_of(weekday, commerce.ADD_TO_CART)
    assert {cell.size for cell in carts["productID"]} == {1}
    orders = rows_of(weekday, commerce.PURCHASE)
    assert min(cell.size for cell in orders["productID"]) == 1
    assert max(cell.size for cell in orders["productID"]) > 1


def test_the_raw_json_agrees_with_the_flat_arrays(weekday: day.Day):
    """Сырое и разобранное — одно событие, поле в поле.

    Сверяется всё, что есть по обе стороны: разъедься хоть одно поле, лаба
    «сырое против разобранного» учила бы неправде. Цена — единственная пара,
    где расхождение законно: в колонке целые рубли, в сыром точная сумма,
    поэтому сверяется не равенство, а что колонка — та же цена, округлённая
    до рубля.
    """
    for kind, action in (
        (commerce.ADD_TO_CART, commerce.ADD_ACTION),
        (commerce.PURCHASE, commerce.PURCHASE_ACTION),
    ):
        rows = rows_of(weekday, kind)
        assert rows["ecommerce"].size > 100
        for number, block in enumerate(raw_of(rows)):
            assert block["currencyCode"] == world.CURRENCY
            products = block[action]["products"]
            for field, column in (
                ("id", "productID"),
                ("name", "productName"),
                ("category", "productCategory"),
                ("quantity", "productQuantity"),
            ):
                assert [item[field] for item in products] == (
                    rows[column][number].tolist()
                ), (kind, field)
            assert rows["productEventType"][number].tolist() == [action] * len(products)
            prices = zip(
                (item["price"] for item in products),
                rows["productPrice"][number].tolist(),
                strict=True,
            )
            assert all(abs(exact - whole) <= 0.5 for exact, whole in prices)

    orders = rows_of(weekday, commerce.PURCHASE)
    for number, block in enumerate(raw_of(orders)):
        field = block[commerce.PURCHASE_ACTION]["actionField"]
        assert field["id"] == orders["purchaseID"][number][0]
        assert field["revenue"] == orders["purchaseRevenue"][number][0]


def test_the_raw_json_carries_what_the_arrays_do_not(weekday: day.Day):
    """Иначе лаба «сырое против разобранного» разбирала бы то же самое."""
    orders = rows_of(weekday, commerce.PURCHASE)
    goods = catalog.catalog()
    brands = set(goods.brand.tolist())
    seen_brands: set[str] = set()
    for block in raw_of(orders):
        for item in block[commerce.PURCHASE_ACTION]["products"]:
            assert item["brand"] in brands
            assert item["variant"]
            seen_brands.add(item["brand"])
    assert len(seen_brands) > 1
    assert not any(
        column.name in ("productBrand", "productVariant") for column in schema.COLUMNS
    )


def test_money_is_whole_kopecks_inside_and_float_only_on_the_surface(weekday: day.Day):
    """Деньги целые; дробное число в событии одно — выручка (спека, раздел 2)."""
    floats = [
        column.name for column in schema.COLUMNS if column.numpy_dtype == "float64"
    ]
    assert floats == ["purchaseRevenue"]

    goods = catalog.catalog()
    orders = rows_of(weekday, commerce.PURCHASE)
    sku = {article: number for number, article in enumerate(goods.sku.tolist())}
    for number, revenue in enumerate(orders["purchaseRevenue"]):
        assert revenue.dtype == np.float64
        kopecks = sum(
            int(goods.price[sku[article]]) * pieces
            for article, pieces in zip(
                orders["productID"][number].tolist(),
                orders["productQuantity"][number].tolist(),
                strict=True,
            )
        )
        assert revenue[0] == kopecks / 100
        assert orders["productPrice"][number].dtype == np.int64


def test_the_rounded_price_does_not_add_up_to_the_revenue(weekday: day.Day):
    """Часть цен несёт копейки, и выручку по массивам события не пересобрать.

    Разрыв живёт внутри одного события: `productPrice` округлён форматом,
    `purchaseRevenue` точна. Это и есть урок про Float64 — не выдуманный.
    """
    orders = rows_of(weekday, commerce.PURCHASE)
    apart = 0
    for number, revenue in enumerate(orders["purchaseRevenue"]):
        by_arrays = float(
            (orders["productPrice"][number] * orders["productQuantity"][number]).sum()
        )
        apart += by_arrays != revenue[0]
    assert 0.1 < apart / orders["WatchID"].size < 0.9


def basket_and_order(events: day.Day) -> tuple[list[int], list[int]]:
    """По каждому заказу — сколько позиций было в корзине и сколько куплено.

    Заодно проверяется само отношение «заказ из корзины»: пустых заказов не
    бывает, купленного мимо корзины не бывает, позиция в заказе одна на товар.
    """
    carts = rows_of(events, commerce.ADD_TO_CART)
    orders = rows_of(events, commerce.PURCHASE)
    put: dict[int, set[str]] = {}
    for visit, article in zip(
        carts["VisitID"].tolist(), (cell[0] for cell in carts["productID"]), strict=True
    ):
        put.setdefault(visit, set()).add(article)

    baskets: list[int] = []
    sizes: list[int] = []
    for visit, bought in zip(
        orders["VisitID"].tolist(), cells(orders["productID"]), strict=True
    ):
        assert bought, "пустых заказов не бывает"
        assert set(bought) <= put[visit], "куплено то, чего не клали в корзину"
        assert len(set(bought)) == len(bought), "позиция в заказе одна на товар"
        baskets.append(len(put[visit]))
        sizes.append(len(bought))
    return baskets, sizes


def test_the_order_is_narrower_than_the_cart(weekday: day.Day):
    """Часть положенных позиций не куплена — иначе сравнивать было бы нечего."""
    baskets, sizes = basket_and_order(weekday)
    assert sum(baskets) > sum(sizes)


def test_the_order_keeps_its_shape_across_the_snapshot(fortnight: list[day.Day]):
    """Состав заказа — доли окна снимка, а не одного дня.

    Заказов в дне около 240, и доля однопозиционных гуляет по дням на
    несколько пунктов от одной случайности выборки; вилки тикета заданы на
    14 днях, там же они и меряются. Список позиций — единственный носитель
    урока про вложенный JSON: схлопнись он в один товар, урок бы умер.
    """
    baskets: list[int] = []
    sizes: list[int] = []
    for events in fortnight:
        was, bought = basket_and_order(events)
        baskets += was
        sizes += bought

    assert 1.6 < sum(sizes) / len(sizes) < 2.2
    assert 0.40 < sum(size == 1 for size in sizes) / len(sizes) < 0.60
    # Брошено не ничего и не всё. Вилка та же, что и до правки: число мира
    # опустилось с 15% до 10%, замер по покупающим корзинам — с 12,0% до
    # 7,2%, и запас до нижней границы остался в полтора раза. Ниже её
    # ронять незачем: с ней сторож замечает, что число мира срезали вдвое,
    # а без неё — уже нет.
    dropped = 1 - sum(sizes) / sum(baskets)
    assert 0.05 < dropped < 0.30


def test_the_cart_holds_only_what_the_visitor_opened(weekday: day.Day):
    """Товар в корзине, которого никто не открывал, — глупость в воронке."""
    order = by_visit(weekday)
    visit = weekday.columns["VisitID"][order]
    kind = weekday.columns["EventType"][order]
    product = weekday.product[order]
    page = weekday.page[order]

    shown: dict[int, set[int]] = {}
    for number, this in enumerate(visit.tolist()):
        if kind[number] == "pageview" and page[number] == Page.PRODUCT:
            shown.setdefault(this, set()).add(int(product[number]))

    put: dict[int, list[int]] = {}
    for number, this in enumerate(visit.tolist()):
        if kind[number] == commerce.ADD_TO_CART:
            put.setdefault(this, []).append(int(product[number]))
    assert put
    for this, products in put.items():
        assert set(products) <= shown[this]
        # Карточка, открытая дважды, даёт одну позицию, а не две.
        assert len(set(products)) == len(products)


def test_putting_something_in_the_cart_is_not_opening_the_cart_page(
    fortnight: list[day.Day],
):
    """Положил и корзину не открыл — самый массовый сюжет магазина.

    До правки два множества совпадали в точности все 14 дней, и факт
    добавления идеально предсказывался более поздним просмотром страницы —
    такого равенства в живых данных не бывает. Сторож меряет долю, а не факт
    различия одним визитом: одно расхождение прошлую ложь не лечит.
    """
    quiet = putting = 0
    for events in fortnight:
        put = visits_that_put(events)
        quiet += len(put - visits_with(events, Page.CART))
        putting += len(put)
    assert quiet / putting > 1 / 3


def test_a_visit_that_opened_the_cart_page_has_something_in_the_cart(
    fortnight: list[day.Day],
):
    """Страница корзины при пустой корзине невозможна.

    Это включение намеренное и остаётся: ломалось обратное. Проверяется на
    обеих неделях — и на буднях, и на выходных: правило про всякий визит, а
    не про удачный день.
    """
    for events in fortnight:
        assert visits_with(events, Page.CART) <= visits_that_put(events)


def test_no_demand_level_is_locked_behind_the_cart_page(fortnight: list[day.Day]):
    """Отвязка от страницы корзины — про весь ассортимент, а не про часть.

    Обнули вероятность целому уровню — и по его товарам вернётся ровно то
    равенство, ради которого правка делалась: «положили — значит, откроют
    корзину», без единого исключения на четверти каталога. Сторож меряет
    долю у каждого уровня, а не факт: одно событие такую примету не лечит.
    """
    goods = catalog.catalog()
    levels = range(len(catalog.DEMAND_LEVELS))
    quiet = [0] * len(catalog.DEMAND_LEVELS)
    put = [0] * len(catalog.DEMAND_LEVELS)
    for events in fortnight:
        shopping = visits_with(events, Page.CART)
        here = events.columns["EventType"] == commerce.ADD_TO_CART
        for visit, number in zip(
            events.columns["VisitID"][here].tolist(),
            events.product[here].tolist(),
            strict=True,
        ):
            put[goods.demand[number]] += 1
            quiet[goods.demand[number]] += visit not in shopping

    for level in levels:
        assert quiet[level] / put[level] > 0.10, catalog.DEMAND_LEVELS[level]


def test_the_demand_level_tells_the_cart_conversion_apart(fortnight: list[day.Day]):
    """Привлекательность товара стала измеримой величиной, а не шумом.

    Знаменатель — разные карточки товаров этого уровня: повторный просмотр
    внутри визита считается один раз, так же как считается позиция. До
    правки решение принималось один раз на визит, и по sku выходил чистый
    шум: внутри воронки 100%, вне её 0%.
    """
    goods = catalog.catalog()
    levels = range(len(catalog.DEMAND_LEVELS))
    shown = [0] * len(catalog.DEMAND_LEVELS)
    put = [0] * len(catalog.DEMAND_LEVELS)
    for events in fortnight:
        for _, number in opened_cards(events):
            shown[goods.demand[number]] += 1
        for number in cart_products(events):
            put[goods.demand[number]] += 1

    share = [put[level] / shown[level] for level in levels]
    magnet, usual, slow = share
    assert magnet > usual > slow
    assert magnet >= 2 * slow


def test_the_cart_events_stay_inside_the_event_budget(fortnight: list[day.Day]):
    """Меняется не сколько кладут, а кто и что: бюджет событий на месте.

    Доля просмотров карточек, дошедших до корзины, — та же величина, что
    держала бюджет до правки; средний день остаётся около 50 тыс. событий
    (спека генератора, разделы 5 и 9).
    """
    put = sum(len(cart_products(events)) for events in fortnight)
    seen = sum(card_views(events) for events in fortnight)
    assert 0.05 < put / seen < 0.09
    average = sum(len(events) for events in fortnight) / len(fortnight)
    assert 48_000 < average < 52_000


def test_a_trade_event_sits_on_the_page_that_sent_it(weekday: day.Day):
    """Корзина — на карточке того самого товара, покупка — на подтверждении.

    Событие не просто стоит после какой-то страницы: у события корзины
    предыдущая строка визита — карточка именно этого товара, и товар в
    массивах события тот же. Иначе в корзину попал бы товар, которого
    посетитель не открывал, а событие село бы на чужую страницу.
    """
    order = by_visit(weekday)
    columns = {name: value[order] for name, value in weekday.columns.items()}
    page, product = weekday.page[order], weekday.product[order]
    kind = columns["EventType"]
    goods = catalog.catalog()
    trade = np.flatnonzero(kind != "pageview")
    assert trade.size > 100

    for row in trade.tolist():
        assert columns["VisitID"][row] == columns["VisitID"][row - 1]
        assert columns["URL"][row] == columns["URL"][row - 1]
        assert columns["Referer"][row] == columns["Referer"][row - 1]
        away = (columns["UTCEventTime"][row] - columns["UTCEventTime"][row - 1]).astype(
            "int64"
        )
        assert 0 < away < world.TRADE_DELAY_SECONDS[1]

        if kind[row] == commerce.ADD_TO_CART:
            assert page[row] == page[row - 1] == Page.PRODUCT
            # Карточка предыдущей строки — карточка этого самого товара.
            assert product[row] == product[row - 1] >= 0
            assert columns["productID"][row].tolist() == [goods.sku[product[row]]]
            # У страницы входа в адресе ещё метки перехода — путь до «?».
            address = columns["URL"][row].split("?")[0]
            assert address.endswith(f"/product/{goods.sku[product[row]]}")
        else:
            assert page[row] == page[row - 1] == Page.CONFIRMATION
            assert kind[row - 1] == "pageview"


def test_the_goals_repeat_the_trade_events(weekday: day.Day):
    """Цели дублируют события — в бою так и бывает (мастер-спека, 1.2)."""
    goals = {
        kind: {tuple(cell.tolist()) for cell in rows_of(weekday, kind)["GoalsReached"]}
        for kind in ("pageview", commerce.ADD_TO_CART, commerce.PURCHASE)
    }
    assert goals["pageview"] == {()}
    assert goals[commerce.ADD_TO_CART] == {(world.GOAL_CART_ID,)}
    assert goals[commerce.PURCHASE] == {(world.GOAL_PURCHASE_ID,)}


def test_the_order_number_reads_as_the_day_and_the_count(weekday: day.Day):
    """Номер — день и порядковый номер покупки в нём; он же `order_id` бэкенда."""
    orders = rows_of(weekday, commerce.PURCHASE)
    numbers = [cell[0] for cell in orders["purchaseID"]]
    date = str(weekday.columns["EventDate"][0]).replace("-", "")
    assert numbers == [f"{date}-{place:04d}" for place in range(1, len(numbers) + 1)]
    assert len(set(numbers)) == len(numbers)
    # Нумерация идёт в порядке событий: строки дня уже упорядочены по времени.
    assert list(orders["UTCEventTime"]) == sorted(orders["UTCEventTime"])


def test_the_coupon_comes_from_the_world_table_and_does_not_touch_the_revenue(
    weekday: day.Day,
):
    """Промокод в событии есть, скидки в сумме нет: клиент шлёт сумму позиций."""
    orders = rows_of(weekday, commerce.PURCHASE)
    known = {code for code, _ in world.COUPONS}
    coupons = [cell[0] for cell in orders["purchaseCoupon"]]
    assert set(coupons) <= known | {""}
    with_coupon = [code for code in coupons if code]
    assert 0.05 < len(with_coupon) / len(coupons) < 0.40
    assert len(set(with_coupon)) > 1

    # Скидка в клиентскую выручку не входит: сумма сходится с позициями и с
    # тем, что уехало в сыром JSON.
    for number, block in enumerate(raw_of(orders)):
        action = block[commerce.PURCHASE_ACTION]["actionField"]
        assert action["revenue"] == orders["purchaseRevenue"][number][0]
        assert ("coupon" in action) == bool(coupons[number])


def test_editing_the_trade_flow_does_not_move_the_traffic(
    monkeypatch: pytest.MonkeyPatch,
):
    """Иерархия подпотоков: у торговли своя случайность (спека, раздел 2)."""
    ours = day.stream(CANONICAL_SEED, WEEKDAY)
    plan.cohort.cache_clear()
    monkeypatch.setattr(world, "ABANDONED_POSITION_PERCENT", 40)
    other = day.stream(CANONICAL_SEED, WEEKDAY)

    def pageviews(events: day.Day) -> dict[str, list[Any]]:
        rows = rows_of(events, "pageview")
        return {
            name: [
                cell.tolist() if isinstance(cell, np.ndarray) else cell
                for cell in value
            ]
            for name, value in rows.items()
        }

    assert pageviews(other) == pageviews(ours)
    assert [
        cell.tolist() for cell in rows_of(other, commerce.PURCHASE)["productID"]
    ] != [cell.tolist() for cell in rows_of(ours, commerce.PURCHASE)["productID"]]


def test_the_trade_flow_draws_from_its_own_named_branch(
    monkeypatch: pytest.MonkeyPatch,
):
    """Торговля берёт подпоток `COMMERCE` — именно его, а не любой другой.

    Неподвижности трафика для этого мало: возьми торговля ветвь трафика,
    трафик всё равно не сдвинулся бы — свои броски он сделал раньше и из
    своего генератора. Разошлись бы только два подпотока, ставшие одним, а
    в дереве зерна место каждого компонента и есть контракт (спека,
    раздел 2). Поэтому проверяется адрес, по которому торговля пришла.
    """
    asked: list[tuple[int, int, Component]] = []
    honest = commerce.day_stream

    def spy(seed: int, day_number: int, component: Component):
        asked.append((seed, day_number, component))
        return honest(seed, day_number, component)

    monkeypatch.setattr(commerce, "day_stream", spy)
    day.stream(CANONICAL_SEED, WEEKDAY)
    assert asked == [(CANONICAL_SEED, WEEKDAY, Component.COMMERCE)]


def test_the_promised_orders_of_a_pair_become_purchases(week: list[day.Day]):
    """Гарантия плана: обе куки пары покупают в назначенные им дни.

    Проверяются пары, у которых оба назначенных дня попали внутрь недели:
    остальные ждут своего дня за горизонтом.
    """
    bought = [
        set(rows_of(events, commerce.PURCHASE)["ClientID"].tolist()) for events in week
    ]
    pairs = 0
    for born in range(-world.RETURN_TAIL_DAYS, WEEK):
        cohort = plan.cohort(CANONICAL_SEED, born)
        for cookies, days in zip(
            cohort.pair_cookies.tolist(), cohort.pair_order_days.tolist(), strict=True
        ):
            if max(days) >= WEEK:
                continue
            pairs += 1
            for cookie, number in zip(cookies, days, strict=True):
                assert int(cohort.client_id[cookie]) in bought[number]
    assert pairs > 10
    # Счётчик пар в плане и пары, реализованные в дне, — про одно и то же.
    assert pairs == plan.counters(CANONICAL_SEED, WEEK).pairs


def paired_cookies() -> set[int]:
    """Куки двухкуковых пар: их заказы обещаны планом, а не решены днём."""
    cookies: set[int] = set()
    for born in range(-world.RETURN_TAIL_DAYS, WEEK):
        cohort = plan.cohort(CANONICAL_SEED, born)
        cookies |= set(cohort.client_id[cohort.pair_cookies.ravel()].tolist())
    return cookies


def test_flagged_buyers_buy_again_much_more_often(week: list[day.Day]):
    """Метка покупателя перестала быть словом: у неё виден след в данных.

    «Постоянный покупатель» читается только так — как кука, которая ходит
    неделями и покупает не раз: метки в событии нет и не будет.

    Куки двухкуковых пар из счёта исключены: их заказы назначил план, и
    вместе с ними лифт мерил бы гарантию, а не два новых рычага. Остаются
    те помеченные, чьи покупки решил день: повторно покупают 4,6% из них
    против 1,5% прочих — втрое чаще при пороге «вдвое».
    """
    purchases: dict[int, int] = {}
    flagged: set[int] = set()
    for number, events in enumerate(week):
        audience = plan.audience(CANONICAL_SEED, number)
        flagged |= set(audience.client_id[audience.buyer].tolist())
        for cookie in rows_of(events, commerce.PURCHASE)["ClientID"].tolist():
            purchases[cookie] = purchases.get(cookie, 0) + 1

    def repeat_share(cookies: set[int]) -> float:
        again = sum(1 for cookie in cookies if purchases[cookie] > 1)
        return again / len(cookies)

    buyers = set(purchases)
    by_levers = (buyers & flagged) - paired_cookies()
    others = buyers - flagged
    assert len(by_levers) > 100
    assert len(others) > 100
    assert repeat_share(by_levers) > 2 * repeat_share(others)


def test_the_shop_stays_the_same_plausible_shop(week: list[day.Day]):
    """Границы правки — вилки спеки (раздел 5): аудитория, объём, конверсия."""
    counters = plan.counters(CANONICAL_SEED, WEEK)
    assert all(6_000 <= size <= 8_000 for size in counters.audience)
    for events in week:
        visits = len(set(events.columns["VisitID"].tolist()))
        orders = int((events.columns["EventType"] == commerce.PURCHASE).sum())
        assert 40_000 < len(events) < 60_000
        assert 0.015 < orders / visits < 0.035
