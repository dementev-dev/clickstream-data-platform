"""Сборка «описания выгрузки» — публичной документации формата события.

Аналог документации Метрики: по нему пишется сторона хранилища (DDL, матвью,
витрины), поэтому документ должен читаться сам по себе, без чтения кода. Всё
содержание берётся из контракта (`schema`), правится только там; свежесть
документа сторожит тест.

Запуск — из каталога генератора целью `make docs`.
"""

import argparse
from itertools import groupby
from pathlib import Path

from clickstream_generator.schema import COLUMNS, Column

PREAMBLE = """# Описание выгрузки: событие кликстрима

Документ собран из контракта схемы генератора
(`generator/src/clickstream_generator/schema.py`). Руками не править —
пересобрать: `make docs` из каталога `generator/`.

Одно событие — одна строка: хит по образцу облачной выгрузки Яндекс Метрики.
Многозначное лежит в параллельных массивах, плюс одно сырое JSON-поле
`ecommerce`. Длина у массивов общая **внутри группы**, а не по всему
событию: `purchase*` — по элементу на заказ (у нас всегда один), `product*` —
по элементу на товар. Отдельной сущности «визит» в выгрузке нет — визиты
собирают на стороне хранилища, а `VisitID` дан как эталон для самопроверки.

Имена и типы колонок — стороны источника. Хранилище принимает их как есть и
нормализует у себя: то же имя в нашем стиле ждёт в столбце «Нормализованное
имя». Это имя источника, приведённое к snake_case, а не имя атрибута в
модели данных: слой DDS складывает свою модель и называет атрибуты по ней.
Столбец «Тип numpy» показывает, чем колонка представлена внутри генератора;
у массивов это тип элемента. Номер — место колонки в выгрузке: порядок задан
контрактом.

Колонки группы «Ecommerce» заполнены только у торговых событий:
`add_to_cart` несёт один товар, `purchase` — состав заказа и блок
`purchase*`. У остальных событий они пусты.

Деньги: `productPrice` — целые рубли, округление формата. Точная сумма
заказа живёт в `purchaseRevenue` и в сыром `ecommerce`, поэтому пересчитать
выручку по разобранным массивам нельзя — цены каталога бывают с копейками.

Всего колонок: {count}."""

TABLE_HEADER = (
    "| № | Колонка | Тип ClickHouse | Тип numpy | Нормализованное имя | Комментарий |",
    "|---|---|---|---|---|---|",
)


def render() -> str:
    """Собирает документ целиком: преамбула и таблица колонок по группам."""
    lines = PREAMBLE.format(count=len(COLUMNS)).splitlines()
    numbers = iter(range(1, len(COLUMNS) + 1))
    for group, columns_of_group in groupby(COLUMNS, key=lambda column: column.group):
        lines += ["", f"## {group.value}", "", *TABLE_HEADER]
        lines += [table_row(next(numbers), column) for column in columns_of_group]
    return "\n".join(lines) + "\n"


def table_row(number: int, column: Column) -> str:
    """Строка таблицы колонок; номер — место колонки в порядке выгрузки."""
    cells = (
        str(number),
        f"`{column.name}`",
        f"`{column.clickhouse_type}`",
        f"`{column.numpy_dtype}`",
        f"`{column.normalized_name}`",
        column.comment,
    )
    return "| " + " | ".join(cells) + " |"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Собирает описание выгрузки из контракта схемы события."
    )
    parser.add_argument("output", type=Path, help="путь к файлу описания")
    output = parser.parse_args().output
    output.write_text(render(), encoding="utf-8")
    print(f"Описание выгрузки собрано: {output}")


if __name__ == "__main__":
    main()
