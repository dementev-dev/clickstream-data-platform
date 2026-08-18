"""День-функция: чистота, форма волны, правила резки визитов, шов с торговлей.

Числа мира тесты сторожат вилками спеки, а не точными значениями: менти
крутит конфигурацию, и падать тесты должны там, где сдвинулся вывод («средний
день ~50 тыс. событий», «ночью провал, вечером пик»), а не при каждой правке.
"""

import inspect
import re
from dataclasses import replace
from typing import Any

import numpy as np
import pytest
from numpy.typing import NDArray

from clickstream_generator import (
    catalog,
    commerce,
    day,
    ids,
    plan,
    reference,
    schema,
    world,
)
from clickstream_generator.reference import Page
from clickstream_generator.seeds import CANONICAL_SEED

WEEKDAY = 2
WEEKEND = 5


@pytest.fixture(autouse=True)
def fresh_memo():
    """Когорты запоминаются; тесты сравнивают вычисления, а не ссылки."""
    plan.cohort.cache_clear()


@pytest.fixture(scope="module")
def weekday() -> day.Day:
    """Один буднийный день на весь модуль: пересчитывать его тестам незачем."""
    return day.stream(CANONICAL_SEED, WEEKDAY)


def readable(column: NDArray[Any]) -> list[Any]:
    """Колонка в сравнимом виде: массив внутри ячейки — в список."""
    if column.dtype == object:
        return [
            cell.tolist() if isinstance(cell, np.ndarray) else cell for cell in column
        ]
    return column.tolist()


def orders(events: day.Day) -> list[list[Any]]:
    """Заказы дня в сравнимом виде: строка на заказ, все его поля."""
    theirs = events.orders
    return [
        [
            theirs.order_id[number],
            int(theirs.user_id[number]),
            theirs.product[number].tolist(),
            theirs.quantity[number].tolist(),
            int(theirs.items_total[number]),
            int(theirs.discount[number]),
            int(theirs.delivery[number]),
            int(theirs.total[number]),
            int(theirs.outcome[number]),
            int(theirs.paid_after[number]),
            int(theirs.cancelled_after[number]),
        ]
        for number in range(len(theirs))
    ]


def snapshot(events: day.Day) -> dict[str, Any]:
    """Слепок дня для сравнений: всё, что день отдал наружу.

    Порядок колонок в слепке живёт отдельным списком: словари сравниваются
    без оглядки на него, а порядок — часть обещания (он же порядок
    контракта схемы). Швы `page` и `product` — тоже часть отдаваемого, как
    и вторая половина дня, заказы: сторожить их надо тем же слепком, а не
    отдельной памятью.
    """
    return {
        "order": list(events.columns),
        "values": [readable(column) for column in events.columns.values()],
        "page": events.page.tolist(),
        "product": events.product.tolist(),
        "orders": orders(events),
    }


def local_seconds(events: day.Day) -> NDArray[np.int64]:
    """Секунда события внутри модельных суток — в поясе счётчика."""
    midnight = np.datetime64(world.ORIGIN, "s") + np.timedelta64(events.day, "D")
    away = np.timedelta64(world.COUNTER_TIMEZONE_MINUTES, "m")
    return (events.columns["UTCEventTime"] - midnight + away).astype("int64")


def hourly(events: day.Day) -> NDArray[np.int64]:
    return np.bincount(local_seconds(events) // 3600, minlength=24)


def test_the_snapshot_notices_everything_the_day_hands_out(weekday: day.Day):
    """Сторож сторожей: слепок обязан замечать порядок колонок и швы.

    Слепок слепой к чему-нибудь — это три зелёных теста детерминизма при
    поехавшем выводе, а не одна пропущенная мелочь.
    """
    original = snapshot(weekday)
    reordered = day.Day(
        day=weekday.day,
        columns=dict(reversed(list(weekday.columns.items()))),
        page=weekday.page,
        product=weekday.product,
        orders=weekday.orders,
    )
    assert snapshot(reordered) != original

    moved = weekday.product.copy()
    moved[0] += 1
    assert snapshot(replace(weekday, product=moved)) != original
    shifted = weekday.page.copy()
    shifted[0] += 1
    assert snapshot(replace(weekday, page=shifted)) != original

    paid = weekday.orders.total.copy()
    paid[0] += 1
    assert snapshot(replace(weekday, orders=replace(weekday.orders, total=paid))) != (
        original
    )

    moment = weekday.orders.paid_after.copy()
    moment[np.flatnonzero(moment >= 0)[0]] += 1
    fated = replace(weekday.orders, paid_after=moment)
    assert snapshot(replace(weekday, orders=fated)) != original


def test_a_day_is_a_pure_function_of_the_seed_and_the_day():
    first = snapshot(day.stream(CANONICAL_SEED, 3))
    plan.cohort.cache_clear()
    assert snapshot(day.stream(CANONICAL_SEED, 3)) == first


def test_a_day_generated_alone_is_the_same_day():
    """День D не зависит от того, прожиты ли дни до него."""
    alone = snapshot(day.stream(CANONICAL_SEED, 3))
    plan.cohort.cache_clear()
    for earlier in range(3):
        day.stream(CANONICAL_SEED, earlier)
    assert snapshot(day.stream(CANONICAL_SEED, 3)) == alone


def test_the_next_day_does_not_move_the_days_before_it():
    before = [snapshot(day.stream(CANONICAL_SEED, number)) for number in range(2)]
    day.stream(CANONICAL_SEED, 2)
    assert [snapshot(day.stream(CANONICAL_SEED, n)) for n in range(2)] == before


def test_another_seed_is_another_day():
    ours = snapshot(day.stream(CANONICAL_SEED, 1))
    assert snapshot(day.stream(CANONICAL_SEED + 1, 1)) != ours


def test_events_do_not_start_before_the_origin():
    with pytest.raises(ValueError):
        day.stream(CANONICAL_SEED, -1)


def test_every_column_of_the_contract_is_present_and_typed(weekday: day.Day):
    """Состав колонок решает контракт схемы; день обязан отдать их все."""
    assert list(weekday.columns) == [column.name for column in schema.COLUMNS]
    for column in schema.COLUMNS:
        values = weekday.columns[column.name]
        assert values.size == len(weekday), column.name
        if column.clickhouse_type.startswith("Array("):
            cells = {cell.dtype for cell in values[:1000]}
            assert cells == {np.dtype(column.numpy_dtype)}, column.name
        else:
            assert values.dtype == np.dtype(column.numpy_dtype), column.name


def test_a_pageview_carries_the_trade_columns_empty_not_missing(weekday: day.Day):
    """Пусто — пустой массив и пустая строка, а ключ есть у каждого события.

    Проверяются все торговые колонки, а не выбранные: у просмотра страницы
    пусты они все до одной, иначе строгий приём хранилища не уживётся с
    полями, пустыми по смыслу (мастер-спека, раздел 6).
    """
    pageview = weekday.columns["EventType"] == "pageview"
    assert pageview.mean() > 0.9
    trade = [
        column
        for column in schema.COLUMNS
        if column.group in (schema.ColumnGroup.ECOMMERCE, schema.ColumnGroup.PARAMS)
    ]
    assert len(trade) > 10
    for column in trade:
        cells = weekday.columns[column.name][pageview][:1000]
        if column.clickhouse_type.startswith("Array("):
            assert all(cell.size == 0 for cell in cells), column.name
        else:
            assert set(cells) == {""}, column.name


def test_the_taxonomy_is_three_event_types(weekday: day.Day):
    """`EventType` — добавка стенда: таксономия нужна явно (мастер-спека, 1.1)."""
    assert set(weekday.columns["EventType"].tolist()) == {
        "pageview",
        commerce.ADD_TO_CART,
        commerce.PURCHASE,
    }


def test_no_column_hides_a_hole(weekday: day.Day):
    """`None` в колонке — та же пропажа ключа, только позже и незаметнее."""
    for column in schema.COLUMNS:
        values = weekday.columns[column.name]
        if values.dtype != object:
            continue
        kind = np.ndarray if column.clickhouse_type.startswith("Array(") else str
        assert all(isinstance(cell, kind) for cell in values[:1000]), column.name


def test_identifiers_survive_json(weekday: day.Day):
    """Числа выше 2^53 в JSON округляются — идентификаторам столько не нужно."""
    for name in ("WatchID", "VisitID", "ClientID"):
        assert weekday.columns[name].max() < ids.LIMIT
        assert weekday.columns[name].dtype == np.uint64


def test_the_event_id_is_unique_because_it_deduplicates(weekday: day.Day):
    """`WatchID` — ключ склейки при переигровке дня (спека, раздел 4)."""
    watch = weekday.columns["WatchID"]
    assert len(set(watch.tolist())) == watch.size


def test_a_visit_belongs_to_one_cookie(weekday: day.Day):
    visit = weekday.columns["VisitID"]
    cookie = weekday.columns["ClientID"]
    pairs = set(zip(visit.tolist(), cookie.tolist(), strict=True))
    assert len(pairs) == len(set(visit.tolist()))


def test_the_visit_cutting_rules_rebuild_the_generator_visits(weekday: day.Day):
    """Правило лабы: та же кука, пауза не длиннее таймаута — тот же визит.

    Сборка сессий по правилам из докстринга модуля обязана совпасть с
    `VisitID` — на этой сверке стоит лаба сессий.
    """
    second = local_seconds(weekday)
    cookie = weekday.columns["ClientID"]
    order = np.lexsort((second, cookie))
    cookie, second = cookie[order], second[order]
    visit = weekday.columns["VisitID"][order]

    started = np.ones(visit.size, dtype=bool)
    started[1:] = (cookie[1:] != cookie[:-1]) | (
        second[1:] - second[:-1] > world.VISIT_TIMEOUT_SECONDS
    )
    rebuilt = np.cumsum(started)
    # Совпадение обоюдное: сколько пар «собранный визит — `VisitID`», столько
    # же и тех, и других. Одного равенства мало — оно ловит только склейку
    # двух визитов в один, а разрыв одного визита надвое проходит мимо.
    matched = set(zip(rebuilt.tolist(), visit.tolist(), strict=True))
    assert len(matched) == int(rebuilt.max())
    assert len(matched) == len(set(visit.tolist()))


def test_the_day_boundary_cuts_the_visits(weekday: day.Day):
    """Событий за границей модельных суток в дне нет — партиция дня целая."""
    second = local_seconds(weekday)
    assert second.min() >= 0
    assert second.max() < day.DAY_SECONDS
    date = np.datetime64(world.ORIGIN, "D") + np.timedelta64(weekday.day, "D")
    assert set(weekday.columns["EventDate"].tolist()) == {date.astype("O")}


def test_the_counter_timezone_moves_the_date_apart_from_utc(weekday: day.Day):
    """`toDate(UTCEventTime)` ≠ `EventDate`: сутки считаются в поясе счётчика."""
    utc_date = weekday.columns["UTCEventTime"].astype("datetime64[D]")
    assert np.any(utc_date != weekday.columns["EventDate"])


def test_the_scale_of_an_average_day_is_about_fifty_thousand(weekday: day.Day):
    """Порядок величины: ~50 тыс. событий в средний день (спека, раздел 5)."""
    assert 30_000 < len(weekday) < 70_000
    visits = len(set(weekday.columns["VisitID"].tolist()))
    assert 6_000 < visits < 14_000


def test_the_wave_dips_at_night_and_peaks_in_the_evening(weekday: day.Day):
    counters = hourly(weekday)
    average = counters.mean()
    assert counters[2:6].max() < average / 2
    assert counters[18:22].max() > 1.5 * average
    assert 0 <= int(counters.argmin()) <= 6
    assert 11 <= int(counters.argmax()) <= 22


def test_the_weekend_is_shaped_unlike_a_weekday(weekday: day.Day):
    """Форма, а не объём: выходной раскачивается позже буднего."""
    weekend = hourly(day.stream(CANONICAL_SEED, WEEKEND))
    workday = hourly(weekday)
    morning = slice(7, 10)
    assert weekend[morning].sum() / weekend.sum() < (
        workday[morning].sum() / workday.sum()
    )


def test_the_funnel_converts_about_two_percent_of_visits(weekday: day.Day):
    visits = len(set(weekday.columns["VisitID"].tolist()))
    ordered = int((weekday.columns["EventType"] == commerce.PURCHASE).sum())
    assert 0.01 < ordered / visits < 0.04


def test_the_promised_orders_reach_the_confirmation(weekday: day.Day):
    """Гарантия двухкуковых пар: назначенный планом заказ обязан состояться."""
    audience = plan.audience(CANONICAL_SEED, weekday.day)
    promised = set(audience.client_id[audience.assigned_order].tolist())
    assert promised
    confirmed = weekday.columns["ClientID"][weekday.page == Page.CONFIRMATION]
    assert promised <= set(confirmed.tolist())


def test_the_cart_always_follows_a_product_card(weekday: day.Day):
    """Шов с торговлей: товар в корзине посетитель до того открывал."""
    order = np.lexsort((local_seconds(weekday), weekday.columns["VisitID"]))
    by_visit = weekday.page[order]
    carts = np.flatnonzero(by_visit == Page.CART)
    assert carts.size > 0
    assert np.all(by_visit[carts - 1] == Page.PRODUCT)


def test_the_product_of_a_card_is_the_seam_for_trade_events(weekday: day.Day):
    """`product` — товар карточки и −1 у прочих страниц; больше ничего."""
    goods = catalog.catalog()
    card = weekday.page == Page.PRODUCT
    assert np.all(weekday.product[~card] == -1)
    assert np.all(weekday.product[card] >= 0)
    assert np.all(weekday.product[card] < goods.sku.size)
    shown = zip(weekday.columns["URL"][card], weekday.product[card], strict=True)
    for url, number in list(shown)[:200]:
        # У страницы входа в адресе ещё метки перехода — путь до знака «?».
        assert url.split("?")[0].endswith(f"/product/{goods.sku[number]}")


def test_the_referer_is_the_page_before(weekday: day.Day):
    """Внутри визита реферер — предыдущий адрес; на входе — адрес источника.

    Цепочка считается по просмотрам страниц: торговое событие не открывает
    страницу, а живёт на уже открытой, и реферер у него тот же, что у неё.
    """
    pageview = weekday.columns["EventType"] == "pageview"
    columns = {name: value[pageview] for name, value in weekday.columns.items()}
    order = np.lexsort((columns["UTCEventTime"], columns["VisitID"]))
    url = columns["URL"][order]
    referer = columns["Referer"][order]
    visit = columns["VisitID"][order]
    inside = np.flatnonzero(visit[1:] == visit[:-1]) + 1
    assert np.all(referer[inside] == url[inside - 1])

    entry = np.flatnonzero(visit[1:] != visit[:-1]) + 1
    known = {source.referer for source in reference.TRAFFIC_SOURCES}
    assert set(referer[entry].tolist()) <= known


def test_the_passport_of_a_cookie_does_not_change_between_days():
    """Кука — браузер на устройстве: во всех её днях профиль и город одни."""
    passports: dict[int, tuple[Any, ...]] = {}
    for number in (0, 1, 2):
        events = day.stream(CANONICAL_SEED, number)
        seen = zip(
            events.columns["ClientID"].tolist(),
            events.columns["Browser"].tolist(),
            events.columns["ScreenWidth"].tolist(),
            events.columns["RegionCity"].tolist(),
            strict=True,
        )
        for cookie, *passport in seen:
            assert passports.setdefault(cookie, tuple(passport)) == tuple(passport)


def test_geography_is_one_region_of_presence(weekday: day.Day):
    """Магазин с одним складом: свой миллионник, свой регион, тонкий хвост."""
    cities = weekday.columns["RegionCity"]
    assert (cities == "Samara").mean() > 0.25
    assert set(weekday.columns["RegionCountry"].tolist()) == {reference.COUNTRY_NAME}
    assert set(weekday.columns["RegionCountryID"].tolist()) == {
        reference.COUNTRY_REGION_ID
    }


def test_addresses_are_not_routable(weekday: day.Day):
    """Правдоподобные публичные адреса принадлежат живым организациям."""
    fixed = ("192.0.2.", "198.51.100.", "203.0.113.", "198.18.")

    def cgnat(address: str) -> bool:
        first, second, *_ = (int(byte) for byte in address.split("."))
        return first == reference.MOBILE_IP_FIRST_BYTE and 64 <= second < 128

    for address in set(weekday.columns["IPAddress"].tolist()):
        assert address.startswith(fixed) or cgnat(address), address


def test_phones_carry_a_model_and_desktops_do_not(weekday: day.Day):
    phone = weekday.columns["DeviceCategory"] == reference.PHONE_CATEGORY
    assert phone.mean() > 0.5
    assert np.all(weekday.columns["MobilePhoneModel"][~phone] == "")
    assert np.all(weekday.columns["MobilePhoneModel"][phone] != "")


def test_randomness_is_drawn_in_whole_numbers():
    """Дисциплина спеки (раздел 2): целые числа, никакой системной математики."""
    source = inspect.getsource(day)
    assert set(re.findall(r"\brng\.(\w+)", source)) <= {"integers"}
    assert not re.search(r"^\s*import (random|math)\b", source, re.MULTILINE)
