"""Каталог товаров: CSV репозитория, общий у генератора и словаря ClickHouse.

Файл `data/catalog/products.csv` один на всех (мастер-спека, раздел 3):
генератор берёт из него имена и цены карточек, а хранилище поднимает над тем
же файлом словарь. Расхождений нет по построению — в бою так же живёт
справочник, выданный источником.

Ассортимент — непродовольственная розница (спека генератора, раздел 9):
числа мира приняты под неё, продуктовая сеть требовала бы других — возврат
раз в неделю, корзина в двадцать позиций.

Что решено формой файла, а не его длиной: колонки `sku,name,category,brand,
price,demand`; артикул — четыре латинские буквы категории, дефис и четыре
цифры; цена — целые копейки (деньги генератор считает целыми, спека,
раздел 2). Часть цен кратна рублю, часть несёт копейки — как в обычной
рознице (1 289,90 ₽). Без копеек урок про Float64 был бы беспредметным:
округлять нечего, и разрыв между `productPrice` и `purchaseRevenue`
пришлось бы выдумывать (спека генератора, раздел 9).
Строк в файле может быть сколько угодно: ни генератор, ни тесты их не
считают, а товар для карточки выбирается равномерно внутри категории.

**Уровень спроса** — привлекательность товара, а не частота его показа:
карточку любого товара открывают одинаково часто, но магнит из открытой
карточки уходит в корзину чаще, чем залежавшийся. Уровень приписан строке
в файле — он постоянная часть мира, а не бросок дня, — и приписан россыпью:
ни с ценой, ни с категорией он не связан. Связь с ценой была бы жёсткой, и
менти нашёл бы в данных ровно то, что мы в них вложили (спека генератора,
раздел 9). Сами вероятности живут в числах мира, здесь только имена уровней.
"""

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
from numpy.typing import NDArray

# Путь от модуля к корню репозитория: генератор живёт в `generator/src/…`.
# Каталог не настраивается извне — он часть мира, а не запуска.
CATALOG_PATH = Path(__file__).resolve().parents[3] / "data" / "catalog" / "products.csv"

COLUMNS = ("sku", "name", "category", "brand", "price", "demand")

# Уровни спроса по убыванию желанности: порядок здесь — порядок чисел мира,
# которыми уровень превращается в вероятность (`world.SHOPPING_ADD_PERCENT`).
DEMAND_LEVELS = ("магнит", "обычный", "залёживается")


@dataclass(frozen=True, slots=True)
class Category:
    """Категория ассортимента: имя в файле, буквы артикула, кусок адреса."""

    name: str
    prefix: str
    slug: str


CATEGORIES = (
    Category("Товары для дома", "HOME", "dlya-doma"),
    Category("Текстиль", "TEXT", "tekstil"),
    Category("Посуда", "POSU", "posuda"),
    Category("Бытовая техника", "TECH", "tehnika"),
    Category("Детские товары", "KIDS", "detskie"),
    Category("Одежда и обувь", "WEAR", "odezhda"),
)


@dataclass(frozen=True, slots=True)
class Catalog:
    """Каталог, разложенный по массивам, плюс указатель на строки категории.

    `grouped` — номера строк, сложенные по категориям подряд; `first` и
    `count` говорят, где чей кусок. Так выбор товара внутри категории —
    один целочисленный бросок, а порядок строк в файле ни на что не влияет.
    """

    sku: NDArray[np.object_]
    name: NDArray[np.object_]
    category: NDArray[np.int64]
    brand: NDArray[np.object_]
    price: NDArray[np.int64]
    demand: NDArray[np.int64]
    grouped: NDArray[np.int64]
    first: NDArray[np.int64]
    count: NDArray[np.int64]


@lru_cache(maxsize=1)
def catalog() -> Catalog:
    """Каталог из файла; читается один раз — он часть постоянного мира."""
    with CATALOG_PATH.open(encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))

    index = {category.name: number for number, category in enumerate(CATEGORIES)}
    category = np.array([index[row["category"]] for row in rows], dtype=np.int64)
    grouped = np.argsort(category, kind="stable")
    count = np.bincount(category, minlength=len(CATEGORIES))
    level = {name: number for number, name in enumerate(DEMAND_LEVELS)}
    return Catalog(
        sku=np.array([row["sku"] for row in rows], dtype=object),
        name=np.array([row["name"] for row in rows], dtype=object),
        category=category,
        brand=np.array([row["brand"] for row in rows], dtype=object),
        price=np.array([int(row["price"]) for row in rows], dtype=np.int64),
        demand=np.array([level[row["demand"]] for row in rows], dtype=np.int64),
        grouped=grouped,
        first=np.concatenate(([0], np.cumsum(count)[:-1])),
        count=count,
    )
