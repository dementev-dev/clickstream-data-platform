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
выручкой), за вычетом строки, которую унесла дельта; `discount` — скидка по
промокоду события от оставшегося `items_total`, по таблице «код → скидка» из
чисел мира; `delivery` — единственные деньги заказа, которых нет ни в одном
событии; `total` — `items_total` − `discount` + `delivery`. Отсюда правило
витрин «деньги считаем по бэкенду»: про скидку и доставку клиент не знает
вовсе.

**Случайность — подпоток заказной стороны**, ветвящийся по дню рождения
заказа: слепок несёт семь дней рождения сразу и судьбу каждого заказа обязан
читать из его собственного дня. Броски делаются на полную длину дня, а не на
отобранных заказах, — иначе длина броска стала бы функцией доли, и правка
одной доли перебрасывала бы весь подпоток после себя
(docs/architecture/orders/fate.md).

**Судьба заказа — исход, его моменты и задержка выгрузки**. Из окна
изменяемости заказ выходит оплаченным или отменённым, а когда именно это
случилось, сказано смещением в секундах от рождения заказа. Момента, которого
у исхода нет, нет и в данных: его место занимает −1, а не ноль, — иначе
«оплатили в секунду рождения» было бы не отличить от «не оплатили вовсе».
Задержка говорит, сколько ранних слепков заказ пропустит; назначенный планом
приезжает сразу.

**Состояние на границе суток** — чтение готовой судьбы, а не накопление:
слепок дня D учитывает моменты не позже границы D|D+1 и по ним называет
статус. Отсюда «дыхание» окна — заказ, оплаченный назавтра, стоит в сегодняшнем
слепке как `created`, а завтра как `paid`.

**Дельта суммы — вычеркнутая позиция**: товара не оказалось в наличии, и
заказ приезжает на строку короче клиентской корзины, а `items_total` меньше
ровно на её полную стоимость. Момента у дельты нет — склад собрал заказ до
первого слепка, поэтому урезан он во всех своих слепках. Заказ из одной
позиции дельты не получает: пустых заказов не бывает.
"""

from dataclasses import dataclass
from enum import IntEnum

import numpy as np
from numpy.typing import NDArray

from clickstream_generator import catalog, world
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
_OUTCOME_CUMULATIVE = np.cumsum(world.ORDER_OUTCOME_WEIGHTS)
_MOMENT_HOUR_CUMULATIVE = np.cumsum(world.ORDER_MOMENT_HOUR_WEIGHTS)
_SNAPSHOT_DELAY_CUMULATIVE = np.cumsum(world.ORDER_SNAPSHOT_DELAY_WEIGHTS)


class OrderOutcome(IntEnum):
    """Чем кончилось окно изменяемости заказа. Порядок — порядок весов мира."""

    PAID = 0
    PAID_THEN_CANCELLED = 1
    UNPAID_THEN_CANCELLED = 2


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
    # Когда строка заказа создана в базе источника: секунда той самой покупки
    # — в модели строка создаётся синхронно с ней — и миллисекунда часов базы.
    # От этого момента отсчитываются и моменты судьбы.
    created_at: NDArray[np.datetime64]
    # Позиции заказа: номера товаров каталога и штуки, ячейка на заказ. У
    # заказа с дельтой позиций на одну меньше, чем в корзине клиента.
    product: tuple[NDArray[np.int64], ...]
    quantity: tuple[NDArray[np.int64], ...]
    # Деньги заказа, целые копейки.
    items_total: NDArray[np.int64]
    discount: NDArray[np.int64]
    delivery: NDArray[np.int64]
    total: NDArray[np.int64]
    # Судьба заказа: исход (`OrderOutcome`) и его моменты — смещения в
    # секундах от рождения заказа, −1 у момента, которого у исхода нет.
    outcome: NDArray[np.int64]
    paid_after: NDArray[np.int64]
    cancelled_after: NDArray[np.int64]
    snapshot_delay: NDArray[np.int64]

    def __len__(self) -> int:
        return self.user_id.size


def of_day(
    seed: int,
    day: int,
    purchases: Purchases,
    assigned_order: NDArray[np.bool_],
) -> Orders:
    """Заказы дня `day`: его покупки, к которым бэкенд добавил свои деньги."""
    rng = day_stream(seed, day, Component.ORDERS)
    delivery = _delivery(rng, len(purchases))
    outcome, paid_after, cancelled_after = _fate(rng, len(purchases))
    product, quantity, items_total = _delta(rng, purchases)
    created_at = _created_at(rng, purchases)
    discount = _discount(purchases, items_total)
    snapshot_delay = _snapshot_delay(rng, assigned_order)
    return Orders(
        day=day,
        order_id=purchases.order_id,
        user_id=purchases.person_id,
        created_at=created_at,
        product=product,
        quantity=quantity,
        items_total=items_total,
        discount=discount,
        delivery=delivery,
        total=items_total - discount + delivery,
        outcome=outcome,
        paid_after=paid_after,
        cancelled_after=cancelled_after,
        snapshot_delay=snapshot_delay,
    )


def window(day: int) -> range:
    """Дни рождения, чьи заказы несёт слепок дня `day`.

    Окно изменяемости — константа мира; у начала оси оно усекается само, а не
    сторожем: дней до D0 попросту нет.
    """
    return range(max(0, day - world.ORDER_WINDOW_DAYS + 1), day + 1)


def at_boundary(rows: Orders, day: int) -> tuple[list[str], NDArray[np.datetime64]]:
    """Статус заказов и момент их последнего изменения на границе `day`|`day+1`.

    Судьба решена при рождении, поэтому слепок её только читает: момент позже
    границы для него ещё не случился. Отмена перевешивает оплату — у дороги
    «оплачен и отменён» она поздняя, и заказ на границе уже отменён.
    `updated_at` — поздний учтённый момент, а без единого заказ показывает своё
    рождение: строку с тех пор никто не трогал.
    """
    edge = _boundary(day)
    paid = rows.created_at + rows.paid_after.astype("timedelta64[s]")
    cancelled = rows.created_at + rows.cancelled_after.astype("timedelta64[s]")
    # Момента, которого у исхода нет, в данных нет вовсе: там −1, и без маски
    # он прикинулся бы моментом за секунду до рождения.
    got_paid = (rows.paid_after >= 0) & (paid <= edge)
    got_cancelled = (rows.cancelled_after >= 0) & (cancelled <= edge)

    status = np.where(got_cancelled, "cancelled", np.where(got_paid, "paid", "created"))
    updated = np.where(
        got_cancelled, cancelled, np.where(got_paid, paid, rows.created_at)
    )
    return status.tolist(), updated


def arrived(rows: Orders, day: int) -> NDArray[np.bool_]:
    """Какие заказы уже попали в выгрузку к слепку дня `day`."""
    return rows.day + rows.snapshot_delay <= day


def _boundary(day: int) -> np.datetime64:
    """Граница суток `day`|`day+1` абсолютной меткой: полночь пояса счётчика.

    Модельные сутки считаются в поясе счётчика, а моменты заказа — метки UTC:
    между ними ровно смещение пояса.
    """
    midnight = np.datetime64(world.ORIGIN, "s") + np.timedelta64(day + 1, "D")
    return midnight - np.timedelta64(world.COUNTER_TIMEZONE_MINUTES, "m")


def _delivery(rng: np.random.Generator, orders: int) -> NDArray[np.int64]:
    """Стоимость доставки каждого заказа дня — броском по таблице весов."""
    return _DELIVERY_PRICE[pick(rng, _DELIVERY_CUMULATIVE, orders)]


def _fate(
    rng: np.random.Generator, orders: int
) -> tuple[NDArray[np.int64], NDArray[np.int64], NDArray[np.int64]]:
    """Исход каждого заказа и моменты, которые этот исход себе оставил.

    Моментов бросается два, и всегда два — обоим исходам с одним моментом
    второй достаётся лишним и выбрасывается. У двух дорог заказа порядок
    выходит сортировкой: ранний момент — оплата, поздний — отмена, — а не
    условной точкой отсчёта, от которой отмеряется вторая.
    """
    outcome = pick(rng, _OUTCOME_CUMULATIVE, orders)
    first, second = _moment(rng, orders), _moment(rng, orders)
    both = outcome == OrderOutcome.PAID_THEN_CANCELLED

    paid = np.where(outcome == OrderOutcome.UNPAID_THEN_CANCELLED, -1, first)
    paid = np.where(both, np.minimum(first, second), paid)
    cancelled = np.where(outcome == OrderOutcome.PAID, -1, first)
    cancelled = np.where(both, np.maximum(first, second), cancelled)
    return outcome, paid, cancelled


def _created_at(
    rng: np.random.Generator, purchases: Purchases
) -> NDArray[np.datetime64]:
    """Момент создания строки заказа: секунда покупки и миллисекунда часов базы.

    Секунда приходит из события — строка создаётся синхронно с покупкой, и
    сдвигать её значило бы подделывать аудит источника. Миллисекунду трекер не
    видит вовсе: у него своё разрешение, у базы своё, и три дописанных нуля
    выдали бы секундную модель за миллисекундную (исследование формата слепка).
    """
    millisecond = rng.integers(0, 1000, len(purchases))
    return purchases.moment.astype("datetime64[ms]") + millisecond.astype(
        "timedelta64[ms]"
    )


def _snapshot_delay(
    rng: np.random.Generator,
    assigned_order: NDArray[np.bool_],
) -> NDArray[np.int64]:
    """Сколько ранних слепков пропустит заказ; назначенный приезжает сразу."""
    delay = pick(rng, _SNAPSHOT_DELAY_CUMULATIVE, assigned_order.size)
    return np.where(assigned_order, 0, delay)


def _moment(rng: np.random.Generator, orders: int) -> NDArray[np.int64]:
    """Момент внутри окна: час по таблице весов и равномерная секунда в нём.

    Без секунды моменты ложились бы на круглые часы, и разности времён в
    аудите давали бы точные равенства там, где их в жизни не бывает.
    """
    hour = pick(rng, _MOMENT_HOUR_CUMULATIVE, orders)
    return hour * 3600 + rng.integers(0, 3600, orders)


def _delta(
    rng: np.random.Generator, purchases: Purchases
) -> tuple[
    tuple[NDArray[np.int64], ...], tuple[NDArray[np.int64], ...], NDArray[np.int64]
]:
    """Позиции заказа и сумма позиций: у выбранных заказов строкой меньше.

    Бросков два, и оба на полную длину дня: признак дельты и номер позиции,
    которую вычеркнул склад. Позиция выбирается равновероятно — корреляцию со
    спросом на такой доле не увидеть ничем, а стоила бы она таблицей чисел
    мира. Верхняя граница у каждого заказа своя, число его позиций:
    `Generator.integers` транслирует массивы границ (стабильная документация
    NumPy, проверено 2026-08-18), поэтому хватает одного векторного броска.

    Однопозиционные заказы запрещаются маской **после** обоих бросков, а не
    отбором до них.
    """
    goods = catalog.catalog()
    count = np.array([line.size for line in purchases.product], dtype=np.int64)
    marked = rng.integers(0, 100, count.size) < world.ORDER_DELTA_PERCENT
    gone = rng.integers(0, count)
    dropped = marked & (count > 1)

    product = list(purchases.product)
    quantity = list(purchases.quantity)
    items_total = purchases.revenue.copy()
    for order in np.flatnonzero(dropped).tolist():
        line = gone[order]
        # Строка уходит целиком, вместе со своей полной стоимостью.
        items_total[order] -= goods.price[product[order][line]] * quantity[order][line]
        product[order] = np.delete(product[order], line)
        quantity[order] = np.delete(quantity[order], line)
    return tuple(product), tuple(quantity), items_total


def _discount(
    purchases: Purchases, items_total: NDArray[np.int64]
) -> NDArray[np.int64]:
    """Скидка каждого заказа: процент промокода от оставшейся корзины, вниз.

    Броска здесь нет: код выбрал посетитель, и он уже уехал в событие —
    бэкенду остаётся прочитать таблицу. Заказ без кода скидки не получает,
    а спорную копейку округление оставляет магазину. Склад сначала вычёркивает
    отсутствующую позицию, поэтому скидка считается уже от `items_total`.
    """
    percent = np.array(
        [_DISCOUNT_PERCENT[code] if code else 0 for code in purchases.coupon],
        dtype=np.int64,
    )
    return items_total * percent // 100
