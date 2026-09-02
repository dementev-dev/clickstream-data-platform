"""Канонический сериализатор: единственное место, где событие целиком → JSON.

Правило «сериализатор один» (спека генератора, разделы 4 и 6) — не про
экономию строк, а про канон: два прогона одного дня обязаны дать те же байты,
а байты рождаются здесь. Второе место, собирающее событие руками, разошлось бы
с этим по экранированию, порядку ключей или записи чисел — и разошлось бы
молча. Граница правила проходит по событию, а не по всякому JSON: вложенный
блок `ecommerce` собирает `commerce`, и это часть содержимого колонки, а не
второй сериализатор.

Что делает канон:

- **Порядок ключей — порядок контракта схемы.** Он берётся из `schema.COLUMNS`
  и нигде не повторяется: два источника порядка разъехались бы при первой же
  вставке колонки.
- **Все 47 ключей всегда.** Пусто по контракту — пустое значение: пустой
  массив, пустая строка, ноль. Пропавший ключ увёл бы событие в брак целиком:
  строгий приём хранилища сверяет набор ключей (ADR 0005).
- **Даты и время — ISO-8601** (спека, раздел 4): `EventDate` уезжает как
  `2026-06-01`, `UTCEventTime` — как `2026-06-01T12:34:56Z`. Довод — читаемость
  сырых данных: менти открывает колонку `raw` обычным клиентом и разбирает событие
  глазами, а число эпохи этот урок убивает.
- **Одно событие — один документ JSON**, без перевода строки внутри: приёмник
  сам решает, чем их разделить.

Колонки переводятся в питоновские значения целиком, а не по строкам: numpy
делает это одним вызовом на колонку, и на дне в полсотни тысяч событий разница
заметна. Обратная сторона — день лежит в памяти дважды; проигрыватель поэтому
и берёт его днями, а не горизонтом целиком.

**Второй контракт провода — слепок заказов** (мастер-спека, раздел 2). Он не
похож на событие: одиннадцать ключей вместо сорока семи, деньги строками, а
не числами, времена с миллисекундами. Общее у них одно, зато главное: байты
рождаются здесь и только здесь. Запись слепка — один словарь с вложенным
списком и один `orjson.dumps`.
"""

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import numpy as np
import orjson
from numpy.typing import NDArray

from clickstream_generator import catalog, schema, world
from clickstream_generator.day import Day
from clickstream_generator.orders import Orders, arrived, at_boundary

_ARRAY_PREFIX = "Array("


def events(day: Day, limit: int | None = None) -> list[bytes]:
    """Канонические байты событий дня: по документу JSON на событие.

    `limit` берёт первые события дня и на этом останавливается — срез для
    того, кто смотрит на конвейер и не хочет ждать целый день (спека,
    раздел 9). Ограничение считается до сериализации: платить за то, что не
    поедет, незачем.
    """
    count = len(day) if limit is None else min(limit, len(day))
    names = tuple(column.name for column in schema.COLUMNS)
    values = [
        _values(column, day.columns[column.name][:count]) for column in schema.COLUMNS
    ]
    return [
        orjson.dumps(dict(zip(names, row, strict=True)))
        for row in zip(*values, strict=True)
    ]


def orders(window: Sequence[Orders], day: int) -> list[bytes]:
    """Канонические байты слепка дня `day`: по документу JSON на заказ.

    `window` — заказы дней окна, от раннего дня к позднему. Уже приехавшие
    заказы слепок несёт подряд, поэтому порядок строк остаётся порядком
    рождения, он же возрастание `order_id`. Какие это дни, решает
    `orders.window`.

    Деньги уезжают строками с ровно двумя знаками, а не числами: у заказа они
    станут `Decimal`, и дробь двоичного числа была бы потерей точности до
    всякого разбора. Времена — метки UTC с миллисекундами; `snapshot_date`
    одинакова во всей выгрузке — это дата дня, состояние которого снято.
    """
    goods = catalog.catalog()
    sku = goods.sku.tolist()
    prices = [_money(price) for price in goods.price.tolist()]
    snapshot_date = (world.ORIGIN + timedelta(days=day)).isoformat()

    payloads = []
    for rows in window:
        status, updated = at_boundary(rows, day)
        created_at = _moments(rows.created_at)
        updated_at = _moments(updated)
        user_id = rows.user_id.tolist()
        items_total = rows.items_total.tolist()
        discount = rows.discount.tolist()
        delivery = rows.delivery.tolist()
        total = rows.total.tolist()

        for number in np.flatnonzero(arrived(rows, day)).tolist():
            order_id = rows.order_id[number]
            payloads.append(
                orjson.dumps(
                    {
                        "order_id": order_id,
                        "user_id": user_id[number],
                        "status": status[number],
                        "created_at": created_at[number],
                        "updated_at": updated_at[number],
                        "items_total": _money(items_total[number]),
                        "discount": _money(discount[number]),
                        "delivery": _money(delivery[number]),
                        "total": _money(total[number]),
                        "items": [
                            {"sku": sku[item], "qty": count, "price": prices[item]}
                            for item, count in zip(
                                rows.product[number].tolist(),
                                rows.quantity[number].tolist(),
                                strict=True,
                            )
                        ],
                        "snapshot_date": snapshot_date,
                    }
                )
            )
    return payloads


def _money(kopecks: int) -> str:
    """Неотрицательные копейки — строкой с двумя знаками: `129990` → `1299.90`."""
    if kopecks < 0:
        raise ValueError(f"деньги не могут быть отрицательными: {kopecks}")
    return f"{kopecks // 100}.{kopecks % 100:02d}"


def _moments(values: NDArray[np.datetime64]) -> list[str]:
    """Метки времени — строками RFC 3339 в UTC с миллисекундами.

    Три знака стоят всегда, в том числе `.000`: одинаковая длина дробной части
    и одинаковая зона дают хронологическую сортировку простым сравнением строк,
    а разбор в хранилище идёт по точному шаблону.
    """
    ms: NDArray[np.datetime64] = values.astype("datetime64[ms]")
    return np.datetime_as_string(ms, unit="ms", timezone="UTC").tolist()


def _values(column: schema.Column, values: NDArray[Any]) -> list[Any]:
    """Колонка питоновскими значениями — в той записи, в какой уедет на провод.

    Массив узнаётся по типу ClickHouse, а не по `numpy_dtype`: у колонки-массива
    там записан тип элемента (`uint32`), и от скалярной колонки её этим не
    отличить.
    """
    if column.clickhouse_type.startswith(_ARRAY_PREFIX):
        # Колонка-массив: в ячейке лежит свой массив, пустой у события,
        # которому эта колонка не по смыслу.
        return [cell.tolist() for cell in values]
    if column.numpy_dtype == "datetime64[D]":
        return np.datetime_as_string(values, unit="D").tolist()
    if column.numpy_dtype == "datetime64[s]":
        return np.datetime_as_string(values, unit="s", timezone="UTC").tolist()
    return values.tolist()
