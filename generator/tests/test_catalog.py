"""Каталог товаров: форма файла, а не его длина.

Файл дорастает механически, поэтому ни один тест не считает его строки и
не знает ни одного артикула наизусть. Сторожится ровно то, на что опираются
генератор и словарь ClickHouse: колонки, вид артикула, известные категории,
целая цена.
"""

import csv
import re

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


def test_the_catalog_is_grouped_by_category_whatever_the_file_order():
    goods = catalog.catalog()
    assert goods.count.sum() == goods.sku.size
    for number in range(len(catalog.CATEGORIES)):
        first = goods.first[number]
        rows_here = goods.grouped[first : first + goods.count[number]]
        assert np.all(goods.category[rows_here] == number)


def test_the_catalog_is_read_once():
    assert catalog.catalog() is catalog.catalog()
