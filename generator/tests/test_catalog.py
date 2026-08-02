"""Каталог товаров: форма файла, а не его длина.

Файл дорастает механически, поэтому ни один тест не считает его строки и
не знает ни одного артикула наизусть. Сторожится ровно то, на что опираются
генератор и словарь ClickHouse: колонки, вид артикула, известные категории,
целая цена.
"""

import collections
import csv
import re
import statistics

import numpy as np

from clickstream_generator import catalog

SKU = re.compile(r"^[A-Z]{4}-\d{4}$")

# Вилка цены, копейки: от сотни рублей до полумиллиона. Сторож от нуля,
# от минуса и от цены, случайно записанной в рублях.
PRICE_RANGE = (10_000, 50_000_000)


def rows() -> list[dict[str, str]]:
    with catalog.CATALOG_PATH.open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source))


def test_the_file_has_the_columns_the_dictionary_expects():
    with catalog.CATALOG_PATH.open(encoding="utf-8", newline="") as source:
        assert next(csv.reader(source)) == list(catalog.COLUMNS)


def test_the_catalog_is_not_empty():
    assert rows()


def test_every_article_is_unique_and_named_by_its_category():
    known = {category.name: category.prefix for category in catalog.CATEGORIES}
    articles = set()
    for row in rows():
        sku = row["sku"]
        assert SKU.match(sku), sku
        assert sku.split("-")[0] == known[row["category"]], sku
        articles.add(sku)
    assert len(articles) == len(rows())


def test_every_row_is_filled_and_priced_in_whole_kopecks():
    low, high = PRICE_RANGE
    for row in rows():
        assert row["name"].strip()
        assert row["brand"].strip()
        assert row["price"].isdigit(), row["price"]
        assert low <= int(row["price"]) <= high, row["sku"]


def test_part_of_the_prices_carry_kopecks():
    """Без копеек урок про Float64 беспредметен: округлять было бы нечего.

    Сторожится не доля, а то, на чём стоит урок: цены бывают и кратные
    рублю, и с копейками — тогда `productPrice` округляется форматом, а
    `purchaseRevenue` несёт точную сумму.
    """
    kopecks = [int(row["price"]) % 100 for row in rows()]
    assert any(rest for rest in kopecks)
    assert any(not rest for rest in kopecks)


def test_every_category_of_the_assortment_is_covered():
    """Каталог покрывает ассортимент целиком: пустых категорий не бывает."""
    present = {row["category"] for row in rows()}
    assert present == {category.name for category in catalog.CATEGORIES}


def test_categories_are_told_apart_by_prefix_and_by_address():
    prefixes = {category.prefix for category in catalog.CATEGORIES}
    slugs = {category.slug for category in catalog.CATEGORIES}
    assert len(prefixes) == len(slugs) == len(catalog.CATEGORIES)


def test_every_row_carries_a_known_demand_level():
    """Уровень спроса есть у каждой строки, и уровней ровно три."""
    known = set(catalog.DEMAND_LEVELS)
    by_category: dict[str, set[str]] = {}
    for row in rows():
        by_category.setdefault(row["category"], set()).add(row["demand"])
    assert set().union(*by_category.values()) == known
    # Уровни рассыпаны по всему ассортименту: категории целиком из магнитов
    # или целиком из залежавшегося в магазине не бывает.
    for category, here in by_category.items():
        assert here == known, category


def test_the_demand_is_scattered_and_not_dealt_out_evenly():
    """Одинаковый расклад в каждой категории — та же неправдоподобная ровность.

    Ровно её тикет и вычищал из данных: в живом магазине магнитов в посуде и
    в одежде не поровну. Уровень тянется из одной колоды на весь каталог,
    поэтому доли категорий расходятся сами.
    """
    magnets: dict[str, int] = {}
    for row in rows():
        magnets.setdefault(row["category"], 0)
        magnets[row["category"]] += row["demand"] == catalog.DEMAND_LEVELS[0]
    assert len(set(magnets.values())) > 1, magnets


def price_tells_the_level(catalogue: list[dict[str, str]]) -> bool:
    """Читается ли уровень спроса по цене товара.

    Спрашивается двумя способами сразу, потому что каждый по отдельности
    обманывается: по четвертям цены — все ли три уровня встречаются в каждой,
    и по медиане цены уровня — не съехала ли она от медианы каталога. Одни
    только края списка ничего не стерегут: четырёх исключений в самых
    дешёвых и самых дорогих строках хватает, чтобы вернуть в них все уровни.
    """
    known = set(catalog.DEMAND_LEVELS)
    by_price = sorted(catalogue, key=lambda row: int(row["price"]))
    step = len(by_price) // 4
    quarters = [by_price[start : start + step] for start in range(0, 4 * step, step)]
    if any({row["demand"] for row in part} != known for part in quarters):
        return True

    whole = statistics.median(int(row["price"]) for row in catalogue)
    for level in catalog.DEMAND_LEVELS:
        here = [int(row["price"]) for row in catalogue if row["demand"] == level]
        if not 0.7 < statistics.median(here) / whole < 1.3:
            return True
    return False


def derived_from_price(catalogue: list[dict[str, str]]) -> list[dict[str, str]]:
    """Подложный каталог: уровень выведен из цены, по краям — исключения.

    Исключения подобраны так, чтобы в самой дешёвой и самой дорогой четверти
    нашлись все три уровня: столько и нужно, чтобы обмануть проверку, которая
    смотрит только на края списка.
    """
    magnet, usual, slow = catalog.DEMAND_LEVELS
    by_price = sorted(catalogue, key=lambda row: int(row["price"]))
    counted = collections.Counter(row["demand"] for row in catalogue)
    deck = [level for level in catalog.DEMAND_LEVELS for _ in range(counted[level])]
    forged = [
        dict(row, demand=level) for row, level in zip(by_price, deck, strict=True)
    ]
    forged[0]["demand"] = slow
    forged[-1]["demand"] = magnet
    forged[-2]["demand"] = usual
    return forged


def test_the_demand_level_is_not_the_price_in_disguise():
    """Отклонённый вариант: выводить склонность из цены (спека, раздел 9).

    Жёсткая связь дала бы менти ровно то, что мы в неё вложили: «дорогое
    залёживается» читалось бы из каталога, а не из данных.
    """
    assert not price_tells_the_level(rows())
    # Сторож сторожа: подмени уровни ценой — проверка обязана покраснеть,
    # и краевые исключения её не должны обманывать.
    assert price_tells_the_level(derived_from_price(rows()))


def test_the_demand_level_reaches_the_arrays_row_by_row():
    goods = catalog.catalog()
    assert [catalog.DEMAND_LEVELS[number] for number in goods.demand.tolist()] == [
        row["demand"] for row in rows()
    ]


def test_the_catalog_is_grouped_by_category_whatever_the_file_order():
    goods = catalog.catalog()
    assert goods.count.sum() == goods.sku.size
    for number in range(len(catalog.CATEGORIES)):
        first = goods.first[number]
        rows_here = goods.grouped[first : first + goods.count[number]]
        assert np.all(goods.category[rows_here] == number)


def test_the_catalog_is_read_once():
    assert catalog.catalog() is catalog.catalog()
