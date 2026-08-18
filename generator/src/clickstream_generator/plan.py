"""План состава: способ спросить у мира, кто в нём есть в день D.

Мир — функция, не состояние. Глобального списка посетителей нет и не будет:
когорта дня — люди, впервые пришедшие именно в этот день, — чистая функция
зерна и номера дня. Аудитория дня складывается из его когорты и возвратов
когорт последних дней: дольше хвоста возвратов кука не живёт, поэтому загляд
назад ограничен окном, а не прожитой историей. День 500 стоит ровно столько
же, сколько день 5 (спека генератора, раздел 1).

Что план решает до генерации событий и чем связывает дни между собой:

- приток — кто и когда впервые появился, и сколько раз вернётся; кука
  помеченного покупателя живёт дольше прочих — это один из двух рычагов
  метки, второй лежит в воронке дня;
- двухкуковые пары — какой человек завёл вторую куку и в какие дни
  каждая из двух кук обязана оформить заказ;
- паспорт куки — устройство и город: они у куки одни и те же во всех её
  днях, а знает об этом только план (у пары один город на двоих);
- личность — `person_id` человека за кукой: у двух кук пары он один, и
  заказ бэкенда показывает его как `user_id`
  (docs/architecture/orders/identity.md);
- счётчики — приток по дням, дневная и накопленная аудитория, пары.

Единица дня здесь — день активности куки, а не визит: слово «визит»
закреплено за сессией и полем `VisitID`, и визитов у куки за день бывает
несколько. Сколько именно — решает день-функция, плану это безразлично.

Случайность тянется целыми числами: диапазоны и выбор по целым весам.
Плавающие распределения системной математики не зовутся — они расходятся
между версиями numpy и архитектурами CPU, а обещано побайтовое совпадение
(спека, раздел 2).
"""

from dataclasses import dataclass, fields
from functools import lru_cache

import numpy as np
from numpy.typing import NDArray

from clickstream_generator import ids, reference, world
from clickstream_generator.seeds import cohort_stream
from clickstream_generator.weights import pick

# Кумулятивные веса: выбор по ним — целочисленный, бросок попадает в чью-то
# долю общего веса.
_RETURN_COUNT_CUMULATIVE = np.cumsum(world.RETURN_COUNT_WEIGHTS)
_BUYER_RETURN_COUNT_CUMULATIVE = np.cumsum(world.BUYER_RETURN_COUNT_WEIGHTS)
_RETURN_DELAY_CUMULATIVE = np.cumsum(world.RETURN_DELAY_WEIGHTS)
_CITY_CUMULATIVE = np.cumsum([city.weight for city in reference.CITIES])
_DEVICE_CUMULATIVE = np.cumsum(
    [profile.weight for profile in reference.DEVICE_PROFILES]
)

# Профили, разложенные надвое — телефоны и всё остальное. Вторая кука пары
# берётся из другой половины: у человека это второе устройство, а не копия
# первого. Мастер-спека (раздел 5) называет пару «телефон и ноутбук»; у нас
# к телефону встаёт десктоп или планшет — вторым устройством бывает и он, а
# запрет на планшет не дал бы ничего, кроме зауженного справочника.
_IS_PHONE = np.array(
    [
        profile.category == reference.PHONE_CATEGORY
        for profile in reference.DEVICE_PROFILES
    ]
)
_DEVICE_HALVES = (np.flatnonzero(~_IS_PHONE), np.flatnonzero(_IS_PHONE))
_DEVICE_HALF_CUMULATIVE = tuple(
    np.cumsum([reference.DEVICE_PROFILES[number].weight for number in half])
    for half in _DEVICE_HALVES
)


@dataclass(frozen=True, slots=True)
class Cohort:
    """Люди, впервые пришедшие в мир в день `day`, с их куками и днями.

    Куки лежат одним рядом: сначала первые куки людей — по одной на человека,
    индексы 0…`people`−1, — затем вторые куки двухкуковых пар. Кука без пары
    и есть человек целиком.

    Дни активности — плоская таблица «кука — день», отсортированная и без
    повторов: дважды за день кука не появляется. Все дни лежат в окне
    активности человека: от `day` до `day` + хвост возвратов.
    """

    day: int
    people: int
    client_id: NDArray[np.uint64]
    birth_day: NDArray[np.int64]
    buyer: NDArray[np.bool_]
    active_cookie: NDArray[np.int64]
    active_day: NDArray[np.int64]
    # Пары и назначенные им заказы — по строке на пару, по колонке на куку.
    pair_cookies: NDArray[np.int64]
    pair_order_days: NDArray[np.int64]
    # Паспорт куки: номер профиля устройства и номер города в справочниках.
    device: NDArray[np.int64]
    city: NDArray[np.int64]
    # Человек за кукой: непрозрачный ID, одинаковый у двух кук пары.
    person_id: NDArray[np.uint64]

    def __post_init__(self) -> None:
        """Когорта запоминается, поэтому массивы отдаются только на чтение.

        Не флагом на самом массиве, а видом на замороженный: флаг
        вызывающий снял бы и сам, а испорченную когорту получили бы потом
        все дни окна. Вид данных не копирует — платы за это нет.

        Граница у защиты честная: полной неприкосновенности numpy не даёт —
        добравшись до владельца данных через `.base`, разморозить можно
        что угодно. Это защита от случайной записи и короткого пути, но не
        от умысла; умысел закрывался бы копией когорты на каждый вызов —
        16 МиБ на день вместо двух миллисекунд.
        """
        for field in fields(self):
            value = getattr(self, field.name)
            if isinstance(value, np.ndarray):
                value.flags.writeable = False
                object.__setattr__(self, field.name, value[...])

    @property
    def pairs(self) -> int:
        """Сколько пар получили назначенные заказы."""
        return len(self.pair_cookies)

    def visitors_on(self, day: int) -> DayAudience:
        """Кто из когорты пришёл в день `day` — её доля дневной аудитории.

        Как дни активности и пары уложены в массивы, знает только когорта:
        снаружи спрашивают день и получают готовые ряды одной длины.
        """
        here = self.active_cookie[self.active_day == day]
        ordering = self.pair_cookies[self.pair_order_days == day]
        return DayAudience(
            day=day,
            client_id=self.client_id[here],
            buyer=self.buyer[here],
            assigned_order=np.isin(here, ordering),
            device=self.device[here],
            city=self.city[here],
            person_id=self.person_id[here],
        )


@dataclass(frozen=True, slots=True)
class DayAudience:
    """Куки, пришедшие в день `day`, — вход дня-функции."""

    day: int
    client_id: NDArray[np.uint64]
    buyer: NDArray[np.bool_]
    # Куки, которым план назначил на этот день гарантированный заказ пары.
    assigned_order: NDArray[np.bool_]
    # Паспорт куки: номера строк в справочниках устройств и городов.
    device: NDArray[np.int64]
    city: NDArray[np.int64]
    # Человек за кукой: его заказ покажет этот ID как `user_id`.
    person_id: NDArray[np.uint64]


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
    # Кука живёт числом ниже 2^53 — тот же потолок, что у номера события:
    # выше JSON округляет при разборе. Граница не достигается.
    client_id = rng.integers(1, ids.LIMIT, cookies, dtype=np.uint64)
    birth_day = np.full(cookies, day, dtype=np.int64)
    # Вторая кука рождается, пока человек ещё ходит: тем же затухающим
    # профилем, что и возвраты, — обычно через дни, изредка через месяцы.
    # Фиксированного зазора нет, иначе пары в данных узнавались бы по нему.
    birth_day[people:] += 1 + pick(rng, _RETURN_DELAY_CUMULATIVE, paired.size)

    # Вторая кука принадлежит покупателю — как и первая кука его пары.
    buyer_cookie = np.concatenate((buyer, np.ones(paired.size, dtype=bool)))
    active_cookie, active_day = _active_days(
        rng, birth_day, buyer_cookie, day + world.RETURN_TAIL_DAYS
    )
    twins = np.column_stack((paired, np.arange(people, cookies, dtype=np.int64)))
    pair_cookies, pair_order_days = _assign_orders(
        rng, twins, active_cookie, active_day, cookies
    )
    # Паспорт бросается последним — после всего, что уже измерено: тогда
    # счётчики канонического мира от этой добавки не двигаются. По тому же
    # правилу за ним приписана личность: этап 3 дописал её в конец, и куки,
    # пары и паспорта остались теми же до байта.
    device, city = _passports(rng, twins, cookies)
    person_id = _persons(rng, twins, people, cookies)
    return Cohort(
        day=day,
        people=people,
        client_id=client_id,
        birth_day=birth_day,
        buyer=buyer_cookie,
        active_cookie=active_cookie,
        active_day=active_day,
        pair_cookies=pair_cookies,
        pair_order_days=pair_order_days,
        device=device,
        city=city,
        person_id=person_id,
    )


def audience(seed: int, day: int) -> DayAudience:
    """Кто пришёл в день `day`: его когорта плюс возвраты когорт окна."""
    if day < 0:
        raise ValueError(f"события начинаются в D0: дня {day} на оси нет")

    # Предыстория ровно такой глубины, чтобы окна хватило и первому дню оси.
    parts = [
        cohort(seed, born).visitors_on(day)
        for born in range(day - world.RETURN_TAIL_DAYS, day + 1)
    ]
    return DayAudience(
        day=day,
        client_id=np.concatenate([part.client_id for part in parts]),
        buyer=np.concatenate([part.buyer for part in parts]),
        assigned_order=np.concatenate([part.assigned_order for part in parts]),
        device=np.concatenate([part.device for part in parts]),
        city=np.concatenate([part.city for part in parts]),
        person_id=np.concatenate([part.person_id for part in parts]),
    )


def counters(seed: int, days: int) -> PlanCounters:
    """Счётчики плана на горизонте `days` — прогон плана по дням, без событий."""
    new_cookies = np.zeros(days, dtype=np.int64)
    daily = np.zeros(days, dtype=np.int64)
    seen: list[NDArray[np.uint64]] = []
    pairs = 0

    # Ниже — вся предыстория: её когорты ещё возвращаются в горизонт.
    for born in range(-world.RETURN_TAIL_DAYS, days):
        born_cohort = cohort(seed, born)
        inside = (born_cohort.active_day >= 0) & (born_cohort.active_day < days)
        daily += np.bincount(born_cohort.active_day[inside], minlength=days)
        seen.append(born_cohort.client_id[np.unique(born_cohort.active_cookie[inside])])
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
    base = world.DAILY_INFLUX * world.WEEKLY_PROFILE_PERCENT[day % 7] // 100
    spread = base * world.INFLUX_JITTER_PERCENT // 100
    return base + int(rng.integers(-spread, spread + 1))


def _active_days(
    rng: np.random.Generator,
    birth_day: NDArray[np.int64],
    buyer: NDArray[np.bool_],
    window_end: int,
) -> tuple[NDArray[np.int64], NDArray[np.int64]]:
    """Дни активности каждой куки: день рождения и возвраты, пока окно открыто.

    Помеченный планом покупатель живёт дольше прочих: одноразовым бывает
    много реже и возвращается чаще. Это первый из двух рычагов метки
    (второй — воронка дня): метки в событии нет, поэтому «постоянный
    покупатель» читается в данных только как кука, которая ходит неделями и
    покупает не раз. Одним лифтом конверсии этого не добиться — кука живёт
    меньше двух визитов за снимок, и второй покупке негде случиться (спека
    генератора, раздел 9).
    """
    cookies = birth_day.size
    returns = np.zeros(cookies, dtype=np.int64)
    one_shot = np.where(buyer, world.BUYER_ONE_SHOT_PERCENT, world.ONE_SHOT_PERCENT)
    returning = rng.integers(0, 100, cookies) >= one_shot
    for here, weights in (
        (returning & ~buyer, _RETURN_COUNT_CUMULATIVE),
        (returning & buyer, _BUYER_RETURN_COUNT_CUMULATIVE),
    ):
        returns[here] = 1 + pick(rng, weights, int(here.sum()))

    owner = np.repeat(np.arange(cookies, dtype=np.int64), returns)
    delay = 1 + pick(rng, _RETURN_DELAY_CUMULATIVE, owner.size)
    cookie = np.concatenate((np.arange(cookies, dtype=np.int64), owner))
    when = np.concatenate((birth_day, birth_day[owner] + delay))

    # Вторая кука пары рождается посреди окна, и её возвраты за край не идут.
    inside = when <= window_end
    cookie, when = cookie[inside], when[inside]
    order = np.lexsort((when, cookie))
    cookie, when = cookie[order], when[order]

    # Два возврата в один день — один день активности: он у куки бывает один.
    first_of_day = np.ones(cookie.size, dtype=bool)
    first_of_day[1:] = (cookie[1:] != cookie[:-1]) | (when[1:] != when[:-1])
    return cookie[first_of_day], when[first_of_day]


def _assign_orders(
    rng: np.random.Generator,
    pair_cookies: NDArray[np.int64],
    active_cookie: NDArray[np.int64],
    active_day: NDArray[np.int64],
    cookies: int,
) -> tuple[NDArray[np.int64], NDArray[np.int64]]:
    """Дни гарантированных заказов пары: по дню каждой из двух кук, от D0.

    Человеку предыстории, у чьей куки дней на оси не осталось, пара не
    назначается: обещать заказ, которого никто не увидит, нечестно. Дни
    берутся той же случайностью, что и всё остальное, — в данных условность
    не видна.
    """
    on_axis = active_day >= 0
    counts = np.bincount(active_cookie[on_axis], minlength=cookies)
    # Дни отсортированы по куке, а внутри куки — по возрастанию, и дни от D0
    # идут последними. Значит, дни на оси у куки — хвост её блока: от конца
    # блока назад ровно `counts` дней.
    first_on_axis = np.searchsorted(active_cookie, np.arange(cookies), "right") - counts

    assigned = np.all(counts[pair_cookies] > 0, axis=1)
    pairs = pair_cookies[assigned]
    chosen = first_on_axis[pairs] + rng.integers(0, counts[pairs])
    return pairs, active_day[chosen]


def _passports(
    rng: np.random.Generator, twins: NDArray[np.int64], cookies: int
) -> tuple[NDArray[np.int64], NDArray[np.int64]]:
    """Устройство и город каждой куки; у пары город один, устройства разные.

    Выводить паспорт арифметикой из `ClientID` было бы дешевле, но про
    двухкуковые пары знает только план, а два города у одного человека —
    ложь в данных (спека генератора, раздел 9).
    """
    device = pick(rng, _DEVICE_CUMULATIVE, cookies)
    city = pick(rng, _CITY_CUMULATIVE, cookies)

    first, second = twins[:, 0], twins[:, 1]
    city[second] = city[first]
    # Половина справочника выбирается по первой куке, строка в ней — броском.
    other_half = np.where(_IS_PHONE[device[first]], 0, 1)
    for half in (0, 1):
        here = second[other_half == half]
        device[here] = _DEVICE_HALVES[half][
            pick(rng, _DEVICE_HALF_CUMULATIVE[half], here.size)
        ]
    return device, city


def _persons(
    rng: np.random.Generator, twins: NDArray[np.int64], people: int, cookies: int
) -> NDArray[np.uint64]:
    """Человек за каждой кукой: у двух кук пары ID один и тот же.

    Личность — минимальная форма отношения «кука принадлежит человеку», и
    знает его только план: заказ спрашивает и показывает то же число как
    `user_id`, а в кликстрим оно не попадает вовсе
    (docs/architecture/orders/identity.md). Значение непрозрачное и живёт
    ниже 2^53 — тот же потолок, что у куки: выше JSON округляет при разборе.
    """
    person_id = np.empty(cookies, dtype=np.uint64)
    person_id[:people] = rng.integers(1, ids.LIMIT, people, dtype=np.uint64)
    # Вторая кука пары повторяет ID своего человека: заказы с двух кук —
    # это и есть склейка, ради которой пары заведены.
    person_id[twins[:, 1]] = person_id[twins[:, 0]]
    return person_id
