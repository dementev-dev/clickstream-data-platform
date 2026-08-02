"""День-функция: (зерно, D) → упорядоченный поток событий модельных суток.

Здесь трафиковая половина мира: визиты, страницы, атрибуция, устройство и
гео. Торговые события садятся на этот же поток и добавляют к нему свои
строки — их собирает `commerce` и им же поток заканчивается. У просмотра
страницы торговые колонки присутствуют, но пусты по смыслу: «пусто» — всегда
пустой массив, пустая строка или 0, а не отсутствие ключа.

День — чистая функция зерна и номера дня: одна и та же пара даёт те же
события, а день N+1 не трогает дни 1…N. Держится это на подпотоке
`Component.TRAFFIC` (спека генератора, раздел 2) и на плане состава, который
про горизонт ничего не знает. Случайность целочисленная и векторная: считать
посточно приходится ровно одно — цепочку страниц визита, где следующий шаг
зависит от предыдущего. Посточные проходы есть и кроме неё (адреса,
заголовки, IP), но там ничего не решается: numpy не умеет собирать строки, а
броски к тому времени уже сделаны — векторно и все разом.

Модельные сутки считаются в поясе счётчика, как в выгрузке Метрики:
`EventDate` — дата в поясе счётчика, `UTCEventTime` — абсолютная метка. У
ночных событий гостей из других поясов `toDate(UTCEventTime)` ≠ `EventDate`;
сторона хранилища должна знать это заранее, иначе выведет дату сама и
разойдётся на несколько часов данных.

**Правила резки визитов** — те же, по которым лаба сессий собирает визиты
сама и сверяет сборку с `VisitID`:

1. Визит принадлежит одной куке: склейка `ClientID` визитом не считается.
2. Пауза дольше 30 минут рвёт визит надвое, поэтому паузы внутри визита
   всегда короче таймаута, а соседние визиты куки разведены дальше него —
   считая от последнего события визита, которым бывает покупка, а не от
   последней его страницы.
3. Граница модельных суток режет визит: события за полночь в дне не живут.
   Исключение одно — визит с заказом, обещанным планом двухкуковых пар: его
   старт сдвигается назад, чтобы воронка уместилась в сутки вместе с
   торговым хвостом. Это принятое ограничение модели: обещание плана —
   гарантия, ради неё мы сужаем свободу старта. Цена названа — около 24
   визитов в день из ~9,5 тыс. не начинаются в последние минуты суток.

**Шов с торговыми событиями.** Поток несёт, кроме колонок, два выровненных
по строкам ряда: `page` — какая это страница магазина, и `product` — какой
товар показывала карточка (−1 у прочих страниц). По ним `commerce` знает и
то, куда сажать событие (карточка, подтверждение), и то, что посетитель на
самом деле смотрел: товар в корзине, которого никто не открывал, — видимая
глупость в воронке. Визит с назначенным заказом всегда доходит до
`/confirmation`, а перед корзиной у него всегда есть карточка товара.
Случайность у половин разная: трафик берёт подпоток `TRAFFIC`, торговля —
`COMMERCE`, и правка одной не сдвигает другую.
"""

from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.typing import NDArray

from clickstream_generator import catalog, commerce, ids, plan, reference, world
from clickstream_generator.reference import Page
from clickstream_generator.seeds import Component, day_stream
from clickstream_generator.weights import pick, pick_row

DAY_SECONDS = 24 * 60 * 60

# Карточка перед корзиной обязательна: положить в корзину то, чего не
# открывал, посетитель не может.
MIN_PAGES_BEFORE_CART = 2

# На столько секунд визит длиннее своих страниц: торговое событие встаёт
# позже страницы, на которую село, и последним событием визита бывает
# покупка, а не просмотр подтверждения. Величина нужна здесь дважды — визит
# с обещанным заказом обязан уместиться в сутки вместе с хвостом, а соседние
# визиты куки разводятся дальше таймаута тоже от хвоста, а не от страницы.
TRADE_TAIL_SECONDS = world.TRADE_DELAY_SECONDS[1]

_VISIT_COUNT_CUMULATIVE = np.cumsum(world.VISITS_PER_ACTIVE_DAY_WEIGHTS)
_VISIT_PAGES_CUMULATIVE = np.cumsum(world.VISIT_PAGES_WEIGHTS)
_SOURCE_CUMULATIVE = np.cumsum([source.weight for source in reference.TRAFFIC_SOURCES])


@dataclass(frozen=True, slots=True)
class Day:
    """Поток событий одного дня: колонки выгрузки и страницы за ними.

    Строки упорядочены по времени — так их и проиграет проигрыватель.
    `columns` — колонки контракта схемы по его порядку, все до одной;
    `page` и `product` выровнены по тем же строкам (см. шов в докстринге
    модуля).
    """

    day: int
    columns: dict[str, NDArray[Any]]
    page: NDArray[np.uint8]
    product: NDArray[np.int64]

    def __len__(self) -> int:
        return self.page.size


@dataclass(frozen=True, slots=True)
class _Visits:
    """Визиты дня рядами: строка — визит, а его страницы лежат подряд.

    Где именно лежат, говорят `first` и `pages`: с какой строки начинается
    визит и сколько их у него. Всё остальное решено до того, как страницы
    сложились в цепочку.
    """

    cookie: NDArray[np.int64]  # номер куки в дневной аудитории
    ordinal: NDArray[np.int64]  # какой это визит куки за день
    ordering: NDArray[np.bool_]  # визит с заказом, обещанным планом
    source: NDArray[np.int64]
    category: NDArray[np.int64]
    stage: NDArray[np.int64]  # докуда дошла воронка
    browse: NDArray[np.int64]  # страниц до воронки
    pages: NDArray[np.int64]
    first: NDArray[np.int64]

    def __len__(self) -> int:
        return self.cookie.size


def stream(seed: int, day: int) -> Day:
    """События дня `day` мира `seed`, упорядоченные по времени."""
    audience = plan.audience(seed, day)
    rng = day_stream(seed, day, Component.TRAFFIC)

    visits = _visits(rng, audience)
    page, product = _walk(rng, visits)
    elapsed, duration = _elapsed(rng, visits)
    start = _starts(rng, day, audience, visits, duration)

    second = np.repeat(start, visits.pages) + elapsed
    # Граница суток режет визит: хвост за полночью в этот день не попадает.
    # Странице подтверждения нужно место и под её покупку: подтверждение без
    # покупки было бы потерей на клиентской стороне, а она здесь честная —
    # потери и дубли стенд заводит намеренно и позже (этапы 4 и 6). Поэтому
    # полночь забирает подтверждение вместе с торговым хвостом или не
    # забирает ни того, ни другого.
    room = np.where(page == Page.CONFIRMATION, TRADE_TAIL_SECONDS, 0)
    alive = second + room < DAY_SECONDS
    visit_id = np.repeat(ids.unique(rng, len(visits)), visits.pages)
    watch_id = ids.unique(rng, int(alive.sum()))

    rest = _columns(rng, day, audience, visits, page, product, second)
    columns = {
        "WatchID": watch_id,
        "VisitID": visit_id[alive],
        **{name: value[alive] for name, value in rest.items()},
    }

    order = np.lexsort((columns["WatchID"], columns["UTCEventTime"]))
    # Торговые события садятся на готовый трафиковый поток и отдают его
    # целиком: в нём же они и упорядочиваются.
    columns, page, product = commerce.weave(
        seed,
        day,
        {name: value[order] for name, value in columns.items()},
        page[alive][order],
        product[alive][order],
    )
    return Day(day=day, columns=columns, page=page, product=product)


def _visits(rng: np.random.Generator, audience: plan.DayAudience) -> _Visits:
    """Визиты дня: сколько их у каждой куки и что в каждом.

    Всё, кроме цепочки страниц: откуда пришли, за какой категорией, докуда
    дойдут по воронке и сколько страниц на это уйдёт.
    """
    counts = 1 + pick(rng, _VISIT_COUNT_CUMULATIVE, audience.client_id.size)
    cookie = np.repeat(np.arange(counts.size, dtype=np.int64), counts)
    ordinal = np.arange(cookie.size) - np.repeat(np.cumsum(counts) - counts, counts)
    visits = cookie.size

    source = pick(rng, _SOURCE_CUMULATIVE, visits)
    category = rng.integers(0, len(catalog.CATEGORIES), visits)
    length = 1 + pick(rng, _VISIT_PAGES_CUMULATIVE, visits)
    # Обещанный планом заказ достаётся первому визиту дня: слева от него
    # соседей нет, поэтому двигать его внутри суток можно свободно.
    ordering = audience.assigned_order[cookie] & (ordinal == 0)
    stage = _funnel(rng, audience.buyer[cookie], ordering)
    # Воронка удлиняет визит, а не съедает его: до корзины надо ещё дойти.
    browse = np.where(
        stage > 0, np.maximum(length - stage, MIN_PAGES_BEFORE_CART), length
    )
    pages = browse + stage
    return _Visits(
        cookie=cookie,
        ordinal=ordinal,
        ordering=ordering,
        source=source,
        category=category,
        stage=stage,
        browse=browse,
        pages=pages,
        first=np.cumsum(pages) - pages,
    )


def _funnel(
    rng: np.random.Generator,
    buyer: NDArray[np.bool_],
    ordering: NDArray[np.bool_],
) -> NDArray[np.int64]:
    """Докуда дошёл визит: 0 — до корзины не дошёл, 3 — до подтверждения.

    Помеченный планом покупатель отличается на обоих шагах: и до корзины
    доходит чаще, и бросает её реже. Склонность покупать — свойство
    человека, а не визита, поэтому одинаковый для всех бросок оставил бы
    метку плана словом без следа в данных (спека генератора, раздел 9).
    """
    draw = rng.integers(0, 100, (3, buyer.size))
    cart = draw[0] < np.where(buyer, world.BUYER_CART_PERCENT, world.CART_PERCENT)
    checkout = cart & (
        draw[1]
        < np.where(
            buyer,
            world.BUYER_CHECKOUT_OF_CART_PERCENT,
            world.CHECKOUT_OF_CART_PERCENT,
        )
    )
    confirmation = checkout & (draw[2] < world.CONFIRMATION_OF_CHECKOUT_PERCENT)

    stage = cart.astype(np.int64) + checkout + confirmation
    # Заказ, обещанный планом, воронку проходит целиком: гарантия пар стоит
    # на том, что событие покупки в этот день случится: его повесит `commerce`.
    stage[ordering] = len(reference.FUNNEL_PAGES)
    return stage


def _walk(
    rng: np.random.Generator, visits: _Visits
) -> tuple[NDArray[np.uint8], NDArray[np.int64]]:
    """Страницы каждого визита и товар их карточек.

    Единственное место дня, где посточен сам расчёт: следующая страница
    зависит от предыдущей, векторно такую цепочку не сложить. Броски
    заготовлены заранее и целиком — в цикле остаётся ходить по таблицам.
    """
    total = int(visits.pages.sum())
    page = np.empty(total, dtype=np.uint8)
    page[visits.first] = _ENTRY_PAGE[visits.source, rng.integers(0, 100, len(visits))]
    step = rng.integers(0, 100, total)
    funnel = np.array(reference.FUNNEL_PAGES, dtype=np.uint8)

    for visit in range(len(visits)):
        begin, browsed = int(visits.first[visit]), int(visits.browse[visit])
        for row in range(begin + 1, begin + browsed):
            previous = page[row - 1]
            table = _NEXT_FROM_PRODUCT if previous == Page.PRODUCT else _NEXT_FROM_LIST
            page[row] = table[step[row]]
        stage = int(visits.stage[visit])
        if stage:
            # В корзину — только с карточки: иначе в ней окажется товар,
            # которого посетитель не открывал.
            page[begin + browsed - 1] = Page.PRODUCT
            page[begin + browsed : begin + browsed + stage] = funnel[:stage]

    goods = catalog.catalog()
    shown = page == Page.PRODUCT
    row_category = np.repeat(visits.category, visits.pages)[shown]
    # Товар — равномерно внутри категории визита: карточки всех товаров
    # открывают одинаково часто, а различает их уровень спроса — уже в
    # корзине, а не в показе (см. `catalog`).
    inside = rng.integers(0, goods.count[row_category])
    product = np.full(total, -1, dtype=np.int64)
    product[shown] = goods.grouped[goods.first[row_category] + inside]
    return page, product


def _elapsed(
    rng: np.random.Generator, visits: _Visits
) -> tuple[NDArray[np.int64], NDArray[np.int64]]:
    """Секунды каждой страницы от начала своего визита и длина визитов."""
    total = int(visits.pages.sum())
    short = rng.integers(*world.PAGE_PAUSE_SECONDS, total)
    long = rng.integers(*world.LONG_PAUSE_SECONDS, total)
    thinking = rng.integers(0, 100, total) < world.LONG_PAUSE_PERCENT
    pause = np.where(thinking, long, short)

    pause[visits.first] = 0
    elapsed = np.cumsum(pause)
    elapsed -= np.repeat(elapsed[visits.first], visits.pages)
    return elapsed, elapsed[visits.first + visits.pages - 1]


def _starts(
    rng: np.random.Generator,
    day: int,
    audience: plan.DayAudience,
    visits: _Visits,
    duration: NDArray[np.int64],
) -> NDArray[np.int64]:
    """Секунда начала каждого визита внутри модельных суток.

    Волна задана в местном времени посетителя, а сутки считаются в поясе
    счётчика: гостю из другого пояса профиль поворачивается на разницу.
    Пик от этого слегка размазывается — как в жизни.
    """
    hour = pick_row(rng, _hour_cumulative(day), audience.city[visits.cookie])
    start = hour * 3600 + rng.integers(0, 3600, hour.size)
    # Визит с обещанным заказом обязан уместиться в сутки целиком — вместе с
    # торговым хвостом: событие покупки встаёт на секунды позже страницы
    # подтверждения, и зажимать его к последней секунде значило бы ломать
    # правило ровно там, ради чего оно написано.
    fits = np.minimum(start, DAY_SECONDS - duration - TRADE_TAIL_SECONDS - 1)
    start = np.where(visits.ordering, fits, start)

    # Визиты куки идут по возрастанию времени и разведены дальше таймаута —
    # иначе лаба склеила бы два визита в один и разошлась бы с `VisitID`.
    # Разводятся они от последнего события визита, а им бывает покупка:
    # считать от последней страницы значило бы отдать таймауту торговый хвост.
    start = start[np.lexsort((start, visits.cookie))]
    for repeat in range(1, len(world.VISITS_PER_ACTIVE_DAY_WEIGHTS)):
        later = np.flatnonzero(visits.ordinal == repeat)
        earlier = later - 1
        start[later] = np.maximum(
            start[later],
            start[earlier]
            + duration[earlier]
            + TRADE_TAIL_SECONDS
            + world.VISIT_TIMEOUT_SECONDS
            + 1,
        )
    return start


def _columns(
    rng: np.random.Generator,
    day: int,
    audience: plan.DayAudience,
    visits: _Visits,
    page: NDArray[np.uint8],
    product: NDArray[np.int64],
    second: NDArray[np.int64],
) -> dict[str, NDArray[Any]]:
    """Колонки выгрузки по строкам-страницам — все, что решил контракт схемы.

    Торговые колонки заполнены пустотой своего типа: пустым массивом и
    пустой строкой. Ключ есть всегда — потребитель не должен гадать, была
    колонка или её забыли.
    """
    total = page.size
    sources = reference.TRAFFIC_SOURCES
    source, pages, first = visits.source, visits.pages, visits.first
    device = audience.device[visits.cookie]
    city = audience.city[visits.cookie]

    url, title = _addresses(rng, visits, page, product)
    # Метки перехода живут и в адресе входа, как в жизни: разбор такого
    # адреса — материал лабы, а колонки UTM рядом дают ей эталон.
    url[first] = [
        f"{address}{'&' if '?' in address else '?'}{query}" if query else address
        for address, query in zip(url[first], _UTM_QUERY[source], strict=True)
    ]
    referer = np.empty(total, dtype=object)
    referer[1:], referer[0] = url[:-1], ""
    referer[first] = _of(sources, "referer")[source]

    def by_visit(values: NDArray[Any]) -> NDArray[Any]:
        return np.repeat(values, pages)

    def by_source(field: str, dtype: Any = object) -> NDArray[Any]:
        return by_visit(_of(sources, field, dtype)[source])

    def by_device(field: str, dtype: Any = object) -> NDArray[Any]:
        return by_visit(_of(reference.DEVICE_PROFILES, field, dtype)[device])

    def by_city(field: str, dtype: Any = object) -> NDArray[Any]:
        return by_visit(_of(reference.CITIES, field, dtype)[city])

    # Дата дня — в поясе счётчика, а полночь того же дня — метка UTC, от
    # которой отсчитываются секунды: между ними ровно смещение пояса.
    date = np.datetime64(world.ORIGIN, "D") + np.timedelta64(day, "D")
    midnight = np.datetime64(world.ORIGIN, "s") + np.timedelta64(day, "D")
    away = np.timedelta64(world.COUNTER_TIMEZONE_MINUTES, "m")
    yclid = np.where(
        _of(sources, "has_yclid", bool)[source],
        rng.integers(1, YCLID_LIMIT, source.size, dtype=np.uint64),
        0,
    ).astype(np.uint64)
    return {
        "ClientID": by_visit(audience.client_id[visits.cookie]),
        "CounterID": np.full(total, world.COUNTER_ID, dtype=np.uint32),
        "EventDate": np.full(total, date, dtype="datetime64[D]"),
        "UTCEventTime": midnight - away + second.astype("timedelta64[s]"),
        "ClientTimeZone": by_city("timezone_minutes", np.int16),
        "EventType": np.full(total, "pageview", dtype=object),
        "Sign": np.ones(total, dtype=np.int8),
        "URL": url,
        "Referer": referer,
        "Title": title,
        "UTMSource": by_source("utm_source"),
        "UTMMedium": by_source("utm_medium"),
        "UTMCampaign": by_source("utm_campaign"),
        "UTMContent": by_source("utm_content"),
        "UTMTerm": by_source("utm_term"),
        "LastTrafficSource": by_source("last_traffic_source"),
        "HasGCLID": by_source("has_gclid", np.uint8),
        "YCLID": by_visit(yclid),
        "Browser": by_device("browser"),
        "BrowserMajorVersion": by_device("browser_major_version", np.uint16),
        "BrowserLanguage": by_device("language"),
        "OperatingSystem": by_device("operating_system"),
        "OperatingSystemRoot": by_device("operating_system_root"),
        "DeviceCategory": by_device("category", np.uint8),
        "MobilePhoneModel": by_device("phone_model"),
        "ScreenWidth": by_device("screen_width", np.uint16),
        "ScreenHeight": by_device("screen_height", np.uint16),
        "IPAddress": by_visit(_ip_addresses(rng, city, device)),
        "RegionCountry": np.full(total, reference.COUNTRY_NAME, dtype=object),
        "RegionCity": by_city("name"),
        "RegionCountryID": np.full(total, reference.COUNTRY_REGION_ID, dtype=np.uint32),
        "RegionCityID": by_city("region_id", np.uint32),
        # Цели дублируют торговые события, поэтому их ставит `commerce`:
        # у просмотра страницы достигнутых целей нет.
        "GoalsReached": _blank(total, "uint32"),
        # Своих параметров сайт стенда пока не шлёт: вариант A/B-теста был бы
        # постоянной куки, а не поведением дня. Решение отложено, не забыто:
        # колонку заполнит #47.
        "ParsedParamsKey1": _blank(total, object),
        "purchaseID": _blank(total, object),
        "purchaseRevenue": _blank(total, "float64"),
        "purchaseCurrency": _blank(total, object),
        "purchaseCoupon": _blank(total, object),
        "productID": _blank(total, object),
        "productName": _blank(total, object),
        "productCategory": _blank(total, object),
        "productPrice": _blank(total, "int64"),
        "productQuantity": _blank(total, "uint64"),
        "productEventType": _blank(total, object),
        "ecommerce": np.full(total, "", dtype=object),
    }


def _addresses(
    rng: np.random.Generator,
    visits: _Visits,
    page: NDArray[np.uint8],
    product: NDArray[np.int64],
) -> tuple[NDArray[np.object_], NDArray[np.object_]]:
    """Адрес и заголовок каждой страницы; у входа в адресе живут метки UTM."""
    total = page.size
    goods = catalog.catalog()
    row_category = np.repeat(visits.category, visits.pages)
    query = rng.integers(0, len(reference.SEARCH_QUERIES), total)
    static = rng.integers(0, len(reference.STATIC_PAGES), total)

    url: list[str] = []
    title: list[str] = []
    for row in range(total):
        kind = page[row]
        if kind == Page.PRODUCT:
            number = product[row]
            path, heading = f"/product/{goods.sku[number]}", goods.name[number]
        elif kind == Page.CATALOG:
            group = catalog.CATEGORIES[row_category[row]]
            path, heading = f"/catalog/{group.slug}", group.name
        elif kind == Page.SEARCH:
            text = reference.SEARCH_QUERIES[query[row]]
            path, heading = f"/search?text={text}", "Поиск по магазину"
        elif kind == Page.STATIC:
            path, heading = reference.STATIC_PAGES[static[row]]
        else:
            path, heading = _FIXED_PAGES[kind]
        url.append(reference.SITE_URL + path)
        title.append(f"{heading} — {reference.SITE_NAME}")
    return np.array(url, dtype=object), np.array(title, dtype=object)


def _ip_addresses(
    rng: np.random.Generator, city: NDArray[np.int64], device: NDArray[np.int64]
) -> NDArray[np.object_]:
    """Адрес визита: городской ломоть, а у телефонов — CGNAT оператора."""
    count = city.size
    prefix = _of(reference.CITIES, "ip_prefix")[city]
    category = _of(reference.DEVICE_PROFILES, "category", np.int64)[device]
    phone = category == reference.PHONE_CATEGORY
    operator = rng.integers(*reference.MOBILE_IP_SECOND_BYTE, count)
    block = rng.integers(0, 256, count)
    host = rng.integers(1, 255, count)

    first_byte = reference.MOBILE_IP_FIRST_BYTE
    return np.array(
        [
            f"{first_byte}.{operator[visit]}.{block[visit]}.{host[visit]}"
            if phone[visit]
            else f"{prefix[visit]}{host[visit]}"
            for visit in range(count)
        ],
        dtype=object,
    )


def _hour_cumulative(day: int) -> NDArray[np.int64]:
    """Веса часов суток по городам, накопленные: строка города — его волна."""
    # Суббота и воскресенье — последние два дня недели, а D0 — понедельник.
    weekend = day % 7 >= 5
    profile = np.array(
        world.WEEKEND_HOURS_PERCENT if weekend else world.WEEKDAY_HOURS_PERCENT
    )
    shifts = [
        (city.timezone_minutes - world.COUNTER_TIMEZONE_MINUTES) // 60
        for city in reference.CITIES
    ]
    return np.cumsum([np.roll(profile, -shift) for shift in shifts], axis=1)


def _of(table: tuple[Any, ...], field: str, dtype: Any = object) -> NDArray[Any]:
    """Колонка справочника массивом: строка выбирается своим номером."""
    return np.array([getattr(row, field) for row in table], dtype=dtype)


def _blank(size: int, dtype: Any) -> NDArray[np.object_]:
    """Колонка-массив, пустая по смыслу: у каждой строки пустой массив."""
    empty = np.array([], dtype=dtype)
    empty.flags.writeable = False
    column = np.empty(size, dtype=object)
    column.fill(empty)
    return column


def _hundred(percent: tuple[int, ...]) -> NDArray[np.uint8]:
    """Сотня ячеек «бросок 0…99 → страница»: выбор одним обращением."""
    return np.repeat(np.array(reference.BROWSE_PAGES, dtype=np.uint8), percent)


def _utm_query(source: reference.TrafficSource) -> str:
    """Метки перехода строкой запроса; у источника без меток — пустая."""
    marks = ((field, getattr(source, f"utm_{field}")) for field in _UTM_FIELDS)
    return "&".join(f"utm_{field}={value}" for field, value in marks if value)


_UTM_FIELDS = ("source", "medium", "campaign", "content", "term")
_UTM_QUERY = np.array(
    [_utm_query(source) for source in reference.TRAFFIC_SOURCES], dtype=object
)

_FIXED_PAGES = {
    Page.HOME: ("/", "Интернет-магазин товаров для дома"),
    Page.CART: ("/cart", "Корзина"),
    Page.CHECKOUT: ("/checkout", "Оформление заказа"),
    Page.CONFIRMATION: ("/confirmation", "Заказ оформлен"),
}

_ENTRY_PAGE = np.array(
    [_hundred(source.entry_percent) for source in reference.TRAFFIC_SOURCES]
)
_NEXT_FROM_LIST = _hundred(reference.FROM_LISTING_PERCENT)
_NEXT_FROM_PRODUCT = _hundred(reference.FROM_PRODUCT_PERCENT)

# Потолок id клика Директа: тринадцать цифр, как у настоящих меток.
YCLID_LIMIT = 10**13
