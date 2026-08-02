"""План состава: способ спросить у мира, кто в нём есть в день D.

Мир — функция, не состояние. Глобального списка посетителей нет и не будет:
когорта дня — люди, впервые пришедшие именно в этот день, — чистая функция
зерна и номера дня. Аудитория дня складывается из его когорты и возвратов
когорт последних дней: дольше хвоста возвратов кука не живёт, поэтому загляд
назад ограничен окном, а не прожитой историей. День 500 стоит ровно столько
же, сколько день 5 (спека генератора, раздел 1).

Что план решает до генерации событий и чем связывает дни между собой:

- приток — кто и когда впервые появился, и сколько раз вернётся;
- двухкуковые пары — какой человек завёл вторую куку и в какие дни
  каждая из двух кук обязана оформить заказ;
- счётчики — приток по дням, дневная и накопленная аудитория, пары.

Случайность тянется целыми числами: диапазоны и выбор по целым весам.
Плавающие распределения системной математики не зовутся — они расходятся
между версиями numpy и архитектурами CPU, а обещано побайтовое совпадение
(спека, раздел 2).
"""

from dataclasses import dataclass, fields
from functools import lru_cache

import numpy as np
from numpy.typing import NDArray

from clickstream_generator import world
from clickstream_generator.seeds import cohort_stream

# Куки живут числами ниже 2^53: выше JSON округляет — тот же довод, что у
# `WatchID` в контракте схемы.
MAX_CLIENT_ID = 2**53

# Кумулятивные веса: выбор по ним — целочисленный, бросок попадает в чью-то
# долю общего веса.
_RETURN_COUNTS = np.cumsum(world.RETURN_COUNT_WEIGHTS)
_RETURN_DELAYS = np.cumsum(world.RETURN_DELAY_WEIGHTS)


@dataclass(frozen=True, slots=True)
class Cohort:
    """Люди, впервые пришедшие в мир в день `day`, с их куками и визитами.

    Куки лежат одним рядом: сначала первые куки людей — по одной на человека,
    индексы 0…`people`−1, — затем вторые куки двухкуковых пар. Кука без пары
    и есть человек целиком.

    Визиты — плоская таблица «кука — день», отсортированная и без повторов:
    на день у куки приходится не больше одного визита. Все дни лежат в окне
    активности человека: от `day` до `day` + хвост возвратов.
    """

    day: int
    people: int
    client_id: NDArray[np.uint64]
    birth_day: NDArray[np.int64]
    buyer: NDArray[np.bool_]
    visit_cookie: NDArray[np.int64]
    visit_day: NDArray[np.int64]
    # Пары и назначенные им заказы — по строке на пару, по колонке на куку.
    pair_cookies: NDArray[np.int64]
    pair_order_days: NDArray[np.int64]

    def __post_init__(self) -> None:
        """Когорта запоминается, поэтому массивы отдаются только на чтение."""
        for field in fields(self):
            value = getattr(self, field.name)
            if isinstance(value, np.ndarray):
                value.flags.writeable = False

    @property
    def pairs(self) -> int:
        """Сколько пар получили назначенные заказы."""
        return len(self.pair_cookies)


@dataclass(frozen=True, slots=True)
class DayAudience:
    """Куки, пришедшие в день `day`, — вход для будущей дня-функции."""

    day: int
    client_id: NDArray[np.uint64]
    buyer: NDArray[np.bool_]
    # Куки, которым план назначил на этот день гарантированный заказ пары.
    assigned_order: NDArray[np.bool_]


@dataclass(frozen=True, slots=True)
class PlanCounters:
    """Счётчики горизонта `days`, известные до генерации хоть одного события."""

    days: int
    new_cookies: tuple[int, ...]
    audience: tuple[int, ...]
    # Накопленная аудитория: uniq(ClientID) за весь горизонт. С дневной не
    # сходится и растёт с горизонтом — это и есть приток, видимый на плане.
    visitors: int
    # Пары, реализованные внутри горизонта: оба назначенных заказа в нём.
    pairs: int


@lru_cache(maxsize=world.RETURN_TAIL_DAYS + 8)
def cohort(seed: int, day: int) -> Cohort:
    """Когорта дня `day`; отрицательный день — предыстория, событий не даёт.

    Функция чистая, а запоминание — только чтобы соседние дни не считали одни
    и те же когорты заново: окно возвратов у них общее.
    """
    if day < -world.RETURN_TAIL_DAYS:
        raise ValueError(f"предыстория мира не глубже {world.RETURN_TAIL_DAYS} дней")

    rng = cohort_stream(seed, day)
    people = _influx(rng, day)
    buyer = rng.integers(0, 100, people) < world.BUYER_PERCENT
    paired = np.flatnonzero(buyer)[
        rng.integers(0, 100, int(buyer.sum())) < world.PAIRED_BUYER_PERCENT
    ]

    cookies = people + paired.size
    client_id = rng.integers(1, MAX_CLIENT_ID, cookies, dtype=np.uint64)
    birth_day = np.full(cookies, day, dtype=np.int64)
    # Вторая кука рождается, пока человек ещё ходит: тем же затухающим
    # профилем, что и возвраты, — обычно через дни, изредка через месяцы.
    # Фиксированного зазора нет, иначе пары в данных узнавались бы по нему.
    birth_day[people:] += 1 + _pick(rng, _RETURN_DELAYS, paired.size)

    visit_cookie, visit_day = _visits(rng, birth_day, day + world.RETURN_TAIL_DAYS)
    pair_cookies, pair_order_days = _assign_orders(
        rng,
        np.column_stack((paired, np.arange(people, cookies, dtype=np.int64))),
        visit_cookie,
        visit_day,
        cookies,
    )
    return Cohort(
        day=day,
        people=people,
        client_id=client_id,
        birth_day=birth_day,
        # Вторая кука принадлежит покупателю — как и первая кука его пары.
        buyer=np.concatenate((buyer, np.ones(paired.size, dtype=bool))),
        visit_cookie=visit_cookie,
        visit_day=visit_day,
        pair_cookies=pair_cookies,
        pair_order_days=pair_order_days,
    )


def audience(seed: int, day: int) -> DayAudience:
    """Кто пришёл в день `day`: его когорта плюс возвраты когорт окна."""
    if day < 0:
        raise ValueError(f"события начинаются в D0: дня {day} на оси нет")

    client_id, buyer, assigned = [], [], []
    # Предыстория ровно такой глубины, чтобы окна хватило и первому дню оси.
    for born in range(day - world.RETURN_TAIL_DAYS, day + 1):
        born_cohort = cohort(seed, born)
        here = born_cohort.visit_cookie[born_cohort.visit_day == day]
        client_id.append(born_cohort.client_id[here])
        buyer.append(born_cohort.buyer[here])
        ordering = born_cohort.pair_cookies[born_cohort.pair_order_days == day]
        assigned.append(np.isin(here, ordering))
    return DayAudience(
        day=day,
        client_id=np.concatenate(client_id),
        buyer=np.concatenate(buyer),
        assigned_order=np.concatenate(assigned),
    )


def counters(seed: int, days: int) -> PlanCounters:
    """Счётчики плана на горизонте `days` — прогон плана по дням, без событий."""
    new_cookies = np.zeros(days, dtype=np.int64)
    daily = np.zeros(days, dtype=np.int64)
    seen: list[NDArray[np.uint64]] = []
    pairs = 0

    # Дальше горизонта когорты не заглядывают, ближе предыстории — не живут.
    for born in range(-world.RETURN_TAIL_DAYS, days):
        born_cohort = cohort(seed, born)
        inside = (born_cohort.visit_day >= 0) & (born_cohort.visit_day < days)
        daily += np.bincount(born_cohort.visit_day[inside], minlength=days)
        seen.append(born_cohort.client_id[np.unique(born_cohort.visit_cookie[inside])])
        appeared = born_cohort.birth_day[
            (born_cohort.birth_day >= 0) & (born_cohort.birth_day < days)
        ]
        new_cookies += np.bincount(appeared, minlength=days)
        pairs += int(np.all(born_cohort.pair_order_days < days, axis=1).sum())

    return PlanCounters(
        days=days,
        new_cookies=tuple(new_cookies.tolist()),
        audience=tuple(daily.tolist()),
        visitors=int(np.unique(np.concatenate(seen)).size),
        pairs=pairs,
    )


def _influx(rng: np.random.Generator, day: int) -> int:
    """Сколько новых людей приходит в этот день: число мира по недельной волне."""
    base = world.DAILY_INFLUX * world.WEEKLY_INFLUX_PERCENT[day % 7] // 100
    spread = base * world.INFLUX_JITTER_PERCENT // 100
    return base + int(rng.integers(-spread, spread + 1))


def _visits(
    rng: np.random.Generator, birth_day: NDArray[np.int64], window_end: int
) -> tuple[NDArray[np.int64], NDArray[np.int64]]:
    """Дни визитов каждой куки: день рождения и возвраты, пока окно открыто."""
    cookies = birth_day.size
    returns = np.zeros(cookies, dtype=np.int64)
    returning = rng.integers(0, 100, cookies) >= world.ONE_SHOT_PERCENT
    returns[returning] = 1 + _pick(rng, _RETURN_COUNTS, int(returning.sum()))

    owner = np.repeat(np.arange(cookies, dtype=np.int64), returns)
    delay = 1 + _pick(rng, _RETURN_DELAYS, owner.size)
    cookie = np.concatenate((np.arange(cookies, dtype=np.int64), owner))
    when = np.concatenate((birth_day, birth_day[owner] + delay))

    # Вторая кука пары рождается посреди окна, и её возвраты за край не идут.
    inside = when <= window_end
    cookie, when = cookie[inside], when[inside]
    order = np.lexsort((when, cookie))
    cookie, when = cookie[order], when[order]

    # Два возврата в один день — один визит: день у куки бывает только один.
    once = np.ones(cookie.size, dtype=bool)
    once[1:] = (cookie[1:] != cookie[:-1]) | (when[1:] != when[:-1])
    return cookie[once], when[once]


def _assign_orders(
    rng: np.random.Generator,
    pair_cookies: NDArray[np.int64],
    visit_cookie: NDArray[np.int64],
    visit_day: NDArray[np.int64],
    cookies: int,
) -> tuple[NDArray[np.int64], NDArray[np.int64]]:
    """Дни гарантированных заказов пары: по визиту каждой из двух кук, от D0.

    Человеку предыстории, у чьей куки визитов на оси не осталось, пара не
    назначается: обещать заказ, которого никто не увидит, нечестно. Дни
    берутся той же случайностью, что и всё остальное, — в данных условность
    не видна.
    """
    on_axis = visit_day >= 0
    counts = np.bincount(visit_cookie[on_axis], minlength=cookies)
    # Визиты куки идут подряд и по возрастанию дня, поэтому дни от D0 — хвост
    # её блока: до конца блока ровно `counts` визитов.
    first_on_axis = np.searchsorted(visit_cookie, np.arange(cookies), "right") - counts

    assigned = np.all(counts[pair_cookies] > 0, axis=1)
    pairs = pair_cookies[assigned]
    chosen = first_on_axis[pairs] + rng.integers(0, counts[pairs])
    return pairs, visit_day[chosen]


def _pick(
    rng: np.random.Generator, cumulative: NDArray[np.int64], size: int
) -> NDArray[np.int64]:
    """Выбор по целым весам: куда попал бросок в общий вес, тот вариант и вышел."""
    return np.searchsorted(cumulative, rng.integers(0, cumulative[-1], size), "right")
