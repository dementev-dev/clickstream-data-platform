"""Заказы бэкенда: вторая проекция покупки и деньги магазина.

Заказ — не второе порождение, а вторая проекция того же факта мира. Корзину,
цены, промокод и номер посчитала торговая половина дня-функции; заказная
половина берёт её покупки готовой структурой и добавляет то, чего у клиента
нет: личность покупателя под родным именем `user_id` и деньги магазина —
скидку по коду, доставку и итог. Новых бросков в торговый подпоток заказная
половина не делает, поэтому два источника согласованы по построению, а не
сверкой (docs/architecture/orders/snapshot.md).

**Деньги — целыми копейками**, как и везде в генераторе: `items_total` —
сумма позиций, посчитанная торговой половиной (у клиента то же число зовётся
выручкой); `discount` — скидка по промокоду события, по таблице «код →
скидка» из чисел мира; `delivery` — единственные деньги заказа, которых нет
ни в одном событии; `total` — `items_total` − `discount` + `delivery`.
Отсюда правило витрин «деньги считаем по бэкенду»: про скидку и доставку
клиент не знает вовсе.

**Случайность — подпоток заказной стороны**, ветвящийся по дню рождения
заказа: слепок несёт семь дней рождения сразу и судьбу каждого заказа обязан
читать из его собственного дня. Броски делаются на полную длину дня, а не на
отобранных заказах, — иначе длина броска стала бы функцией доли, и правка
одной доли перебрасывала бы весь подпоток после себя
(docs/architecture/orders/fate.md).

Судьбы у заказа здесь ещё нет: статус, моменты оплаты и отмены и дельта
суммы — следующий тикет.
"""

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from clickstream_generator import world
from clickstream_generator.commerce import Purchases
from clickstream_generator.seeds import Component, day_stream
from clickstream_generator.weights import pick

_DISCOUNT_PERCENT = dict(world.COUPONS)
_DELIVERY_PRICE = np.array(
    [price for price, _ in world.DELIVERY_KOPECKS_WEIGHTS], dtype=np.int64
)
_DELIVERY_CUMULATIVE = np.cumsum(
    [weight for _, weight in world.DELIVERY_KOPECKS_WEIGHTS]
)


@dataclass(frozen=True, slots=True)
class Orders:
    """Заказы одного модельного дня: ряды одной длины, по элементу на заказ.

    Порядок — порядок рождения заказов, он же возрастание `order_id`. День
    рождения заказа — `day`: строка в базе источника создаётся синхронно с
    покупкой, поэтому день создания строки и день покупки совпадают.
    """

    day: int
    order_id: tuple[str, ...]
    # Пользователь магазина: тот же человек, что стоит за купившей кукой.
    user_id: NDArray[np.uint64]
    # Позиции заказа: номера товаров каталога и штуки, ячейка на заказ.
    product: tuple[NDArray[np.int64], ...]
    quantity: tuple[NDArray[np.int64], ...]
    # Деньги заказа, целые копейки.
    items_total: NDArray[np.int64]
    discount: NDArray[np.int64]
    delivery: NDArray[np.int64]
    total: NDArray[np.int64]

    def __len__(self) -> int:
        return self.user_id.size


def of_day(seed: int, day: int, purchases: Purchases) -> Orders:
    """Заказы дня `day`: его покупки, к которым бэкенд добавил свои деньги."""
    rng = day_stream(seed, day, Component.ORDERS)
    delivery = _delivery(rng, len(purchases))
    discount = _discount(purchases)
    return Orders(
        day=day,
        order_id=purchases.order_id,
        user_id=purchases.person_id,
        product=purchases.product,
        quantity=purchases.quantity,
        items_total=purchases.revenue,
        discount=discount,
        delivery=delivery,
        total=purchases.revenue - discount + delivery,
    )


def _delivery(rng: np.random.Generator, orders: int) -> NDArray[np.int64]:
    """Стоимость доставки каждого заказа дня — броском по таблице весов."""
    return _DELIVERY_PRICE[pick(rng, _DELIVERY_CUMULATIVE, orders)]


def _discount(purchases: Purchases) -> NDArray[np.int64]:
    """Скидка каждого заказа: процент промокода от суммы позиций, вниз.

    Броска здесь нет: код выбрал посетитель, и он уже уехал в событие —
    бэкенду остаётся прочитать таблицу. Заказ без кода скидки не получает,
    а спорную копейку округление оставляет магазину.
    """
    percent = np.array(
        [_DISCOUNT_PERCENT[code] if code else 0 for code in purchases.coupon],
        dtype=np.int64,
    )
    return purchases.revenue * percent // 100
