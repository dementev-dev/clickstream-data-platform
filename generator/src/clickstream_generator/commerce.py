"""Торговые события: корзина и заказ на потоке дня.

Здесь торговая половина мира. Событие корзины садится на карточку товара —
в жизни его шлёт кнопка на карточке, а не открытие страницы корзины; при
нескольких товарах в заказе иначе его и не разложить: событий столько, со
скольких карточек положили. Событие покупки садится на страницу
подтверждения. Оба встают на несколько секунд позже своей страницы, а
граница модельных суток режет всё, что за неё вышло: визит, у которого
подтверждение срезано полуночью, покупки не даёт — заказа не было.

**Кладёт любой визит, открывший карточку.** Положить и корзину не открыть —
самый массовый сюжет магазина, поэтому событие корзины не привязано к
странице `/cart`: иначе факт добавления идеально предсказывался бы более
поздним просмотром страницы, а такого равенства в живых данных не бывает.
Решается каждая открытая карточка отдельно, а не визит целиком, и решают её
двое: намерение визита и уровень спроса товара (числа мира). Карточка,
открытая дважды, даёт одну позицию — повторный просмотр это раздумье, а не
второй товар.

**Корзина шире заказа.** Покупает посетитель не всё, что положил: часть
позиций остаётся брошенной. Иначе событие корзины не рассказывало бы ничего
сверх покупки — заказ был бы её точной копией, и сравнивать было бы нечего.

**Деньги считаются целыми копейками.** В колонку `productPrice` ложатся
целые рубли, как у Метрики, а дробное число в событии одно —
`purchaseRevenue`. Часть цен каталога несёт копейки, поэтому округление
видно: выручку по разобранным массивам не пересчитать, точная сумма живёт в
`purchaseRevenue` и в сыром `ecommerce`. Разрыв внутри одного события — это
настоящий урок формата, а не придуманный.

**Промокод в событии есть, скидки в сумме нет.** Выручка, которую шлёт
клиент, — сумма позиций без скидки и доставки: код на сайте знает корзину, а
не итог расчёта. Скидку по коду насчитывает бэкенд (этап 3), и таблица «код
→ скидка» лежит в числах мира, чтобы обе стороны брали одну.

**Номер заказа читаемый** — день модельного времени и порядковый номер
покупки в этом дне (`20260603-0042`). Он же `order_id` бэкенда: по нему
соединяется сверка (мастер-спека, раздел 4). Нумеруются все покупки,
дошедшие до потока дня, в порядке событий — до всяких потерь; поэтому номер
присваивается последним ходом, когда поток уже упорядочен.

**Второй выход — покупки дня.** Кроме потока событий торговая половина
отдаёт покупки готовой структурой: номер заказа, корзина, деньги клиента,
промокод и человек за кукой. Заказная сторона берёт их такими и ничего не
пересчитывает: заказ — вторая проекция той же покупки, поэтому согласие двух
источников не удерживается, а выходит по построению
(docs/architecture/orders/snapshot.md).

**Случайность — подпоток `COMMERCE`** (спека генератора, раздел 2): правка
торгового поведения не сдвигает трафиковый поток. Броски целые и векторные;
посточно собираются только строки — их numpy не умеет. Новых бросков заказы
сюда не добавляют: свои решения заказная сторона тянет из своего подпотока.
"""

from dataclasses import dataclass
from typing import Any

import numpy as np
import orjson
from numpy.typing import NDArray

from clickstream_generator import catalog, ids, reference, world
from clickstream_generator.reference import Page
from clickstream_generator.seeds import Component, day_stream
from clickstream_generator.weights import pick

# Таксономия: тип события и действие с товаром. Полный словарь торговых
# событий Метрики (detail, remove, impressions) стенд не берёт.
ADD_TO_CART = "add_to_cart"
PURCHASE = "purchase"
ADD_ACTION = "add"
PURCHASE_ACTION = "purchase"

# Копеек в рубле: внутри генератора деньги целые, в колонках — рубли.
KOPECKS = 100

_QUANTITY_CUMULATIVE = np.cumsum(world.ITEM_QUANTITY_WEIGHTS)

_PRODUCT_COLUMNS = (
    "productID",
    "productName",
    "productCategory",
    "productPrice",
    "productQuantity",
    "productEventType",
)


@dataclass(frozen=True, slots=True)
class _Baskets:
    """Корзины дня: позиции по корзинам и строка подтверждения заказа.

    Позиция — товар, положенный в корзину: `anchor` — строка карточки, на
    которую сядет событие, `product` — номер товара в каталоге. Позиции
    лежат подряд по корзинам, внутри корзины — по времени; `basket` говорит,
    чья позиция, а `first` и `count` — где чей кусок. `confirmation`
    отвечает на вопрос, дошла ли корзина до заказа: строка подтверждения
    или −1 у брошенной.
    """

    anchor: NDArray[np.int64]
    product: NDArray[np.int64]
    basket: NDArray[np.int64]
    first: NDArray[np.int64]
    count: NDArray[np.int64]
    confirmation: NDArray[np.int64]

    def __len__(self) -> int:
        return self.confirmation.size

    def positions_of(self, basket: int, kept: NDArray[np.bool_]) -> NDArray[np.int64]:
        """Позиции корзины, у которых стоит отметка: например, купленные."""
        here = slice(self.first[basket], self.first[basket] + self.count[basket])
        return self.first[basket] + np.flatnonzero(kept[here])


@dataclass(frozen=True, slots=True)
class _Draws:
    """Броски торгового подпотока: случайное решается один раз и разом.

    По позициям корзин — сколько штук берут, какой это вариант товара,
    дошла ли позиция до заказа и на сколько секунд событие корзины отстало
    от карточки. По корзинам — промокод и задержка события покупки.
    """

    quantity: NDArray[np.int64]
    variant: NDArray[np.int64]
    kept: NDArray[np.bool_]
    cart_delay: NDArray[np.int64]
    coupon: NDArray[np.int64]
    order_delay: NDArray[np.int64]


@dataclass(frozen=True, slots=True)
class Purchases:
    """Покупки дня готовой структурой — второй выход торговой половины.

    Ряды одной длины, по элементу на покупку, в порядке номеров заказа.
    Корзина лежит парой рядов ячеек — номера товаров каталога и штуки; в
    каждой ячейке свой массив, длиной в число позиций покупки.
    """

    order_id: tuple[str, ...]
    # Человек за кукой, которая купила: у заказа он назовётся `user_id`.
    person_id: NDArray[np.uint64]
    product: tuple[NDArray[np.int64], ...]
    quantity: tuple[NDArray[np.int64], ...]
    # Промокод события строкой; у покупки без кода — пустая.
    coupon: tuple[str, ...]
    # Деньги клиента целыми копейками: сумма позиций без скидки и доставки,
    # та самая, что уехала в событие как `purchaseRevenue`. Бэкенд назовёт
    # её `items_total` — у двух источников свои имена одному числу.
    revenue: NDArray[np.int64]

    def __len__(self) -> int:
        return self.person_id.size


@dataclass(frozen=True, slots=True)
class _Events:
    """Строки одного вида торговых событий, готовые встать в поток.

    `raw` — сырой `ecommerce` каждой строки ещё объектом: номер заказа в нём
    появится, когда поток будет упорядочен, а строка станет байтами один
    раз, каноническим сериализатором. `basket` говорит, чья корзина стоит за
    строкой: по нему покупка находит свою после того, как поток упорядочен.
    """

    columns: dict[str, NDArray[Any]]
    page: NDArray[np.uint8]
    product: NDArray[np.int64]
    raw: list[dict[str, Any]]
    basket: NDArray[np.int64]


# Что торговая половина отдаёт дню: поток целиком — колонки, страницы и
# товары — и покупки дня вторым выходом.
Woven = tuple[dict[str, NDArray[Any]], NDArray[np.uint8], NDArray[np.int64], Purchases]

# День без единой корзины: покупок в нём нет, а форма у рядов есть.
_NO_PURCHASES = Purchases(
    order_id=(),
    person_id=np.empty(0, dtype=np.uint64),
    product=(),
    quantity=(),
    coupon=(),
    revenue=np.empty(0, dtype=np.int64),
)


def weave(
    seed: int,
    day: int,
    columns: dict[str, NDArray[Any]],
    page: NDArray[np.uint8],
    product: NDArray[np.int64],
    person: NDArray[np.uint64],
) -> Woven:
    """Вплетает торговые события в поток и отдаёт поток и покупки дня.

    Строки приходят упорядоченными по времени и такими же уходят: торговые
    события встают между ними, и поток пересобирается одним порядком. Второй
    выход — покупки дня: то же самое, чем они уехали в события.

    `person` — человек за кукой каждой строки, выровненный по входящему
    потоку: личность приходит от плана состава, а не выводится из трекера
    (docs/architecture/orders/identity.md).
    """
    rng = day_stream(seed, day, Component.COMMERCE)
    baskets = _baskets(rng, page, product, columns["VisitID"])
    if not len(baskets):
        return columns, page, product, _NO_PURCHASES

    goods = catalog.catalog()
    draws = _draws(rng, baskets)
    revenue = _revenue(goods, baskets, draws)
    coupons = _coupon_codes(draws)
    events = (
        _cart_events(columns, baskets, goods, draws),
        _order_events(columns, baskets, goods, draws, revenue, coupons),
    )
    columns, page, product, basket = _stream(rng, columns, page, product, events)
    purchases = _purchases(columns, basket, baskets, draws, revenue, coupons, person)
    return columns, page, product, purchases


def _draws(rng: np.random.Generator, baskets: _Baskets) -> _Draws:
    """Всё случайное в торговых событиях — целыми числами и векторно."""
    positions = baskets.product.size
    return _Draws(
        quantity=1 + pick(rng, _QUANTITY_CUMULATIVE, positions),
        variant=rng.integers(0, len(reference.PRODUCT_VARIANTS), positions),
        kept=_kept(rng, baskets),
        cart_delay=rng.integers(*world.TRADE_DELAY_SECONDS, positions),
        coupon=_coupons(rng, len(baskets)),
        order_delay=rng.integers(*world.TRADE_DELAY_SECONDS, len(baskets)),
    )


def _baskets(
    rng: np.random.Generator,
    page: NDArray[np.uint8],
    product: NDArray[np.int64],
    visit: NDArray[np.uint64],
) -> _Baskets:
    """Что посетитель положил в корзину и дошёл ли до заказа.

    Корзина есть у всякого визита, положившего хоть что-то, — не только у
    дошедшего до страницы `/cart`. Кандидаты — карточки, открытые в визите,
    по одной на товар: карточка, открытая дважды, даёт одну позицию, и
    остаётся первая — тогда товар и кладут.

    Намерение визита берётся из шага воронки: тот, кто дошёл до страницы
    корзины, пришёл покупать. Позиция у него есть всегда — страница корзины
    при пустой корзине невозможна, — и держится это высокой вероятностью, а
    не принуждением: спасать приходится считаные проценты таких визитов.
    Принуждай мы всех, корзина схлопнулась бы до одной позиции, а заказ до
    одного товара.
    """
    order = np.argsort(visit, kind="stable")
    page, product, visit = page[order], product[order], visit[order]

    # Строки визита лежат подряд, внутри визита — по времени: поток пришёл
    # упорядоченным, а сортировка по визиту устойчивая.
    started = np.concatenate(([True], visit[1:] != visit[:-1]))
    number = np.cumsum(started) - 1
    visits = int(started.sum())

    goods = catalog.catalog()
    cards = np.flatnonzero(page == Page.PRODUCT)
    # Ключ «визит и товар»: `np.unique` отдаёт индексы первых вхождений,
    # поэтому вторая карточка того же товара кандидата не добавляет.
    key = number[cards] * goods.sku.size + product[cards]
    opened = np.sort(cards[np.unique(key, return_index=True)[1]])
    holder = number[opened]

    shopping = np.zeros(visits, dtype=np.bool_)
    shopping[number[page == Page.CART]] = True
    taken = _taken(rng, goods.demand[product[opened]], shopping[holder])
    # Кандидаты визита лежат подряд, визиты идут по порядку — поэтому начало
    # каждого куска находится накопленной суммой.
    opened_count = np.bincount(holder, minlength=visits)
    opened_first = np.cumsum(opened_count) - opened_count
    empty = shopping & (np.bincount(holder[taken], minlength=visits) == 0)
    _at_least_one(rng, taken, opened_first, opened_count, empty)

    positions, owner = opened[taken], holder[taken]
    with_basket = np.unique(owner)
    basket_of_visit = np.full(visits, -1, dtype=np.int64)
    basket_of_visit[with_basket] = np.arange(with_basket.size)

    # Подтверждение без корзины невозможно: до него визит прошёл страницу
    # корзины, а у неё позиция есть всегда — поэтому номер корзины найдётся.
    confirmed = np.flatnonzero(page == Page.CONFIRMATION)
    confirmation = np.full(with_basket.size, -1, dtype=np.int64)
    confirmation[basket_of_visit[number[confirmed]]] = order[confirmed]

    basket = basket_of_visit[owner]
    count = np.bincount(basket, minlength=with_basket.size)
    return _Baskets(
        anchor=order[positions],
        product=product[positions],
        basket=basket,
        # Позиции лежат подряд, корзины идут по порядку визитов — начало
        # каждого куска находится той же накопленной суммой.
        first=np.cumsum(count) - count,
        count=count,
        confirmation=confirmation,
    )


def _taken(
    rng: np.random.Generator,
    demand: NDArray[np.int64],
    shopping: NDArray[np.bool_],
) -> NDArray[np.bool_]:
    """Дошла ли открытая карточка до корзины: решают намерение и товар.

    Два ряда чисел мира по уровням спроса: один для визита, пришедшего
    покупать, другой для того, кто просто смотрит. Товар решает у обоих, но
    у смотрящего он решает почти всё — потому конверсия «карточка → корзина»
    по уровням спроса и становится различимой.
    """
    chance = np.where(
        shopping,
        np.array(world.SHOPPING_ADD_PERCENT)[demand],
        np.array(world.BROWSING_ADD_PERCENT)[demand],
    )
    return rng.integers(0, 100, demand.size) < chance


def _kept(rng: np.random.Generator, baskets: _Baskets) -> NDArray[np.bool_]:
    """Какие позиции корзины дошли до заказа: часть остаётся брошенной.

    Пустым заказ не бывает: если брошены все позиции, одна возвращается.
    """
    kept = (
        rng.integers(0, 100, baskets.product.size) >= world.ABANDONED_POSITION_PERCENT
    )
    empty = np.bincount(baskets.basket[kept], minlength=len(baskets)) == 0
    _at_least_one(rng, kept, baskets.first, baskets.count, empty)
    return kept


def _at_least_one(
    rng: np.random.Generator,
    chosen: NDArray[np.bool_],
    first: NDArray[np.int64],
    count: NDArray[np.int64],
    empty: NDArray[np.bool_],
) -> None:
    """Возвращает один выбор тем группам, у которых бросок унёс все.

    Приём общий у двух мест: у визита выбираются карточки, у корзины —
    позиции, а правило одно — пустой ни та, ни другая быть не может. Какой
    выбор вернуть, решает свой бросок: брать первый значило бы, что ранняя
    карточка визита переживает любой бросок. Группа без членов сюда не
    приходит: у визита со страницей корзины карточка была, у корзины —
    позиция.
    """
    chosen[first[empty] + rng.integers(0, count[empty])] = True


def _coupons(rng: np.random.Generator, baskets: int) -> NDArray[np.int64]:
    """Промокод корзины: номер строки в таблице кодов или −1, если кода нет."""
    code = rng.integers(0, len(world.COUPONS), baskets)
    return np.where(rng.integers(0, 100, baskets) < world.COUPON_PERCENT, code, -1)


def _coupon_codes(draws: _Draws) -> NDArray[np.object_]:
    """Промокод каждой корзины строкой; у корзины без кода — пустая.

    Код нужен обеим сторонам: событие везёт его как есть, а бэкенд по нему
    считает скидку заказа — по той же таблице чисел мира.
    """
    return np.array(
        [
            world.COUPONS[number][0] if number >= 0 else ""
            for number in draws.coupon.tolist()
        ],
        dtype=object,
    )


def _revenue(
    goods: catalog.Catalog, baskets: _Baskets, draws: _Draws
) -> NDArray[np.int64]:
    """Деньги клиента по корзинам, целыми копейками: сумма купленных позиций.

    Сумма считается один раз на день и уезжает в оба источника: в событие —
    как `purchaseRevenue`, в заказ — как `items_total`. Разойтись им негде,
    и это не совпадение, а построение.
    """
    value = goods.price[baskets.product] * draws.quantity
    kopecks = np.zeros(len(baskets), dtype=np.int64)
    np.add.at(kopecks, baskets.basket[draws.kept], value[draws.kept])
    return kopecks


def _cart_events(
    columns: dict[str, NDArray[Any]],
    baskets: _Baskets,
    goods: catalog.Catalog,
    draws: _Draws,
) -> _Events:
    """Строки `add_to_cart`: по одной на каждый положенный товар."""
    positions = baskets.product.size
    alone = [np.array([position]) for position in range(positions)]

    rows = _on_page(columns, baskets.anchor, ADD_TO_CART, draws.cart_delay)
    rows["GoalsReached"] = _same(
        np.array([world.GOAL_CART_ID], dtype=np.uint32), positions
    )
    side, blocks = _product_side(goods, baskets, alone, draws, ADD_ACTION)
    rows.update(side)

    return _Events(
        columns=rows,
        page=np.full(positions, Page.PRODUCT, dtype=np.uint8),
        product=baskets.product.copy(),
        raw=[{"currencyCode": world.CURRENCY, ADD_ACTION: block} for block in blocks],
        basket=baskets.basket,
    )


def _order_events(
    columns: dict[str, NDArray[Any]],
    baskets: _Baskets,
    goods: catalog.Catalog,
    draws: _Draws,
    revenue: NDArray[np.int64],
    coupons: NDArray[np.object_],
) -> _Events:
    """Строки `purchase`: по одной на корзину, дошедшую до подтверждения."""
    ordered = np.flatnonzero(baskets.confirmation >= 0)
    bought = [baskets.positions_of(basket, draws.kept) for basket in ordered]

    rows = _on_page(
        columns, baskets.confirmation[ordered], PURCHASE, draws.order_delay[ordered]
    )
    rows["GoalsReached"] = _same(
        np.array([world.GOAL_PURCHASE_ID], dtype=np.uint32), ordered.size
    )
    side, blocks = _product_side(goods, baskets, bought, draws, PURCHASE_ACTION)
    rows.update(side)

    # Выручка клиента — сумма позиций без скидки и доставки, целыми копейками.
    # Считана она один раз на день: тем же числом её возьмёт заказ бэкенда.
    kopecks = revenue[ordered].tolist()
    codes = coupons[ordered].tolist()
    rows["purchaseRevenue"] = _cells(
        [np.array([money / KOPECKS], dtype=np.float64) for money in kopecks]
    )
    rows["purchaseCurrency"] = _same(
        np.array([world.CURRENCY], dtype=object), ordered.size
    )
    rows["purchaseCoupon"] = _cells([np.array([code], dtype=object) for code in codes])

    raw = [
        {
            "currencyCode": world.CURRENCY,
            PURCHASE_ACTION: {"actionField": _action_field(money, code), **block},
        }
        for money, code, block in zip(kopecks, codes, blocks, strict=True)
    ]
    return _Events(
        columns=rows,
        page=np.full(ordered.size, Page.CONFIRMATION, dtype=np.uint8),
        product=np.full(ordered.size, -1, dtype=np.int64),
        raw=raw,
        basket=ordered,
    )


def _action_field(kopecks: int, code: str) -> dict[str, Any]:
    """Блок `actionField` заказа: номер, выручка и купон, если он был.

    Номер пустой до сборки потока — его присваивает `_seal`, когда порядок
    событий дня уже известен.
    """
    field: dict[str, Any] = {"id": "", "revenue": kopecks / KOPECKS}
    if code:
        field["coupon"] = code
    return field


def _product_side(
    goods: catalog.Catalog,
    baskets: _Baskets,
    groups: list[NDArray[np.int64]],
    draws: _Draws,
    action: str,
) -> tuple[dict[str, NDArray[Any]], list[dict[str, Any]]]:
    """Массивы `product*` и товарная часть сырого JSON — одним проходом.

    Массивы группы `product*` одной длины между собой: по элементу на товар.
    С группой `purchase*` они не совпадают и не должны — там по элементу на
    заказ (мастер-спека, раздел 1).

    Сырой JSON несёт больше, чем колонки: бренд, вариант товара и точную
    цену с копейками. На этом и стоит лаба «сырое против разобранного» —
    иначе в сыром лежало бы ровно то же самое.
    """
    columns: dict[str, list[NDArray[Any]]] = {name: [] for name in _PRODUCT_COLUMNS}
    blocks: list[dict[str, Any]] = []
    for group in groups:
        numbers = baskets.product[group]
        pieces = draws.quantity[group]
        columns["productID"].append(np.array(goods.sku[numbers], dtype=object))
        columns["productName"].append(np.array(goods.name[numbers], dtype=object))
        columns["productCategory"].append(
            np.array([_category(goods, number) for number in numbers], dtype=object)
        )
        # Цена в колонке — целые рубли, как у Метрики: это округление и есть
        # тот разрыв, из-за которого выручку по массивам не пересобрать.
        columns["productPrice"].append((goods.price[numbers] + KOPECKS // 2) // KOPECKS)
        columns["productQuantity"].append(pieces.astype(np.uint64))
        columns["productEventType"].append(np.full(group.size, action, dtype=object))
        blocks.append(
            {
                "products": [
                    {
                        "id": goods.sku[number],
                        "name": goods.name[number],
                        "category": _category(goods, number),
                        "brand": goods.brand[number],
                        "variant": reference.PRODUCT_VARIANTS[draws.variant[position]],
                        "price": int(goods.price[number]) / KOPECKS,
                        "quantity": int(pieces[place]),
                    }
                    for place, (position, number) in enumerate(
                        zip(group.tolist(), numbers.tolist(), strict=True)
                    )
                ]
            }
        )
    return {name: _cells(values) for name, values in columns.items()}, blocks


def _category(goods: catalog.Catalog, number: int) -> str:
    """Имя категории товара — то же, что в файле каталога и в словаре."""
    return catalog.CATEGORIES[goods.category[number]].name


def _on_page(
    columns: dict[str, NDArray[Any]],
    anchor: NDArray[np.int64],
    event_type: str,
    delay: NDArray[np.int64],
) -> dict[str, NDArray[Any]]:
    """Событие на странице: её колонки целиком, свой тип и своё время.

    Страница у торгового события та же, что у просмотра, на который оно
    село: тот же адрес и реферер, та же кука, тот же визит, устройство и
    гео. Различаются тип, время и торговые колонки — их кладёт вызывающий.
    """
    rows = {name: value[anchor] for name, value in columns.items()}
    rows["EventType"] = np.full(anchor.size, event_type, dtype=object)
    rows["UTCEventTime"] = columns["UTCEventTime"][anchor] + delay.astype(
        "timedelta64[s]"
    )
    return rows


def _stream(
    rng: np.random.Generator,
    columns: dict[str, NDArray[Any]],
    page: NDArray[np.uint8],
    product: NDArray[np.int64],
    events: tuple[_Events, ...],
) -> tuple[
    dict[str, NDArray[Any]], NDArray[np.uint8], NDArray[np.int64], NDArray[np.int64]
]:
    """Собирает поток дня целиком: сутки режут хвост, время задаёт порядок.

    Четвёртым рядом уходит корзина каждой строки — у просмотра страницы её
    нет: по ней покупка находит свою корзину, когда поток уже упорядочен и
    полночь свой хвост отрезала.
    """
    traffic = page.size
    trade = {
        name: np.concatenate([part.columns[name] for part in events])
        for name in columns
    }
    raw = [block for part in events for block in part.raw]

    alive = trade["UTCEventTime"] < _midnight(columns) + np.timedelta64(1, "D")
    trade = {name: value[alive] for name, value in trade.items()}
    # Номера торговых строк — из торгового подпотока: возьми их день у
    # трафика, и правка торгового поведения сдвинула бы трафиковые `WatchID`.
    trade["WatchID"] = ids.unique_apart_from(rng, int(alive.sum()), columns["WatchID"])
    raw = [block for block, here in zip(raw, alive.tolist(), strict=True) if here]

    rows = {
        name: np.concatenate((value, trade[name])) for name, value in columns.items()
    }
    page = np.concatenate((page, np.concatenate([part.page for part in events])[alive]))
    product = np.concatenate(
        (product, np.concatenate([part.product for part in events])[alive])
    )
    # Чей сырой блок в какой строке: у просмотра страницы блока нет.
    place = np.full(traffic + len(raw), -1, dtype=np.int64)
    place[traffic:] = np.arange(len(raw))

    basket = np.concatenate(
        (
            np.full(traffic, -1, dtype=np.int64),
            np.concatenate([part.basket for part in events])[alive],
        )
    )

    order = np.lexsort((rows["WatchID"], rows["UTCEventTime"]))
    rows = {name: value[order] for name, value in rows.items()}
    _seal(rows, raw, place[order], _order_prefix(columns))
    return rows, page[order], product[order], basket[order]


def _seal(
    rows: dict[str, NDArray[Any]],
    raw: list[dict[str, Any]],
    place: NDArray[np.int64],
    prefix: str,
) -> None:
    """Раздаёт номера заказов и собирает сырой `ecommerce`.

    Номер получают все покупки, дошедшие до потока, в порядке событий — до
    всяких потерь: этап 6 выбрасывает событие, когда номер уже присвоен,
    иначе одна потеря перенумеровала бы чужие заказы и мост к бэкенду
    разъехался бы. Строку собирает канонический сериализатор: руками это был
    бы второй сериализатор со своим экранированием.
    """
    number = 0
    for row in np.flatnonzero(place >= 0).tolist():
        block = raw[place[row]]
        if rows["EventType"][row] == PURCHASE:
            number += 1
            code = f"{prefix}-{number:04d}"
            block[PURCHASE_ACTION]["actionField"]["id"] = code
            rows["purchaseID"][row] = np.array([code], dtype=object)
        rows["ecommerce"][row] = orjson.dumps(block).decode()


def _purchases(
    rows: dict[str, NDArray[Any]],
    basket: NDArray[np.int64],
    baskets: _Baskets,
    draws: _Draws,
    revenue: NDArray[np.int64],
    coupons: NDArray[np.object_],
    person: NDArray[np.uint64],
) -> Purchases:
    """Покупки дня — то же, чем они уехали в события, только структурой.

    Порядок здесь — порядок строк потока, он же порядок номеров заказа:
    поток уже упорядочен и пронумерован, полночь свой хвост уже отрезала.
    Человек берётся у строки подтверждения — визит принадлежит одной куке,
    а кука одному человеку.
    """
    here = np.flatnonzero(rows["EventType"] == PURCHASE)
    mine = basket[here]
    bought = [baskets.positions_of(number, draws.kept) for number in mine.tolist()]
    return Purchases(
        order_id=tuple(rows["purchaseID"][row][0] for row in here.tolist()),
        person_id=person[baskets.confirmation[mine]],
        product=tuple(baskets.product[group] for group in bought),
        quantity=tuple(draws.quantity[group] for group in bought),
        coupon=tuple(coupons[mine].tolist()),
        revenue=revenue[mine],
    )


def _midnight(columns: dict[str, NDArray[Any]]) -> np.datetime64:
    """Начало модельных суток абсолютной меткой: полночь в поясе счётчика."""
    date: np.datetime64 = columns["EventDate"][0]
    return date.astype("datetime64[s]") - np.timedelta64(
        world.COUNTER_TIMEZONE_MINUTES, "m"
    )


def _order_prefix(columns: dict[str, NDArray[Any]]) -> str:
    """Первая половина номера заказа: день модельного времени, `20260603`."""
    return str(columns["EventDate"][0]).replace("-", "")


def _cells(values: list[NDArray[Any]]) -> NDArray[np.object_]:
    """Колонка-массив: в каждой ячейке свой массив своего типа."""
    column = np.empty(len(values), dtype=object)
    for row, value in enumerate(values):
        column[row] = value
    return column


def _same(value: NDArray[Any], size: int) -> NDArray[np.object_]:
    """Колонка-массив, у которой во всех ячейках один и тот же массив."""
    value.flags.writeable = False
    column = np.empty(size, dtype=object)
    column.fill(value)
    return column
