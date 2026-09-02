"""Проверка паспорта мира: совпадает ли он с тем, что собирается из кода сегодня.

Паспорт собирается из кода, значит разойтись они могут только одним способом —
код правили, паспорт не пересобрали. Ровно это здесь и сторожится, тем же
способом, что свежесть «описания выгрузки».

Проверка дорогая — она пересчитывает восемь модельных дней целиком, и это
единственный способ сравнить хеши: дешевле мир не пересобрать. Зато краснеет
она там, где надо, — сразу после правки генератора, а не через полчаса на
поднятом стенде, где расхождение счётчиков выглядит поломкой хранилища.
"""

import hashlib
import json
from pathlib import Path

import pytest

from clickstream_generator import cli
from clickstream_generator import day as day_module
from clickstream_generator.inventory import STARTING_DAYS, build
from clickstream_generator.seeds import CANONICAL_SEED

INVENTORY_PATH = Path(__file__).resolve().parents[2] / "data" / "world-inventory.json"

# Прогон, слепок которого сверяется с паспортом байт в байт. День любой из
# отправляемых; этот дешевле прочих — его окно короче.
RUN_DAY = 2

# День любой: хеш дня не зависит ни от соседей, ни от края паспорта.
REPLAY_DAY = 4


@pytest.fixture(scope="module")
def inventory() -> dict:
    """Паспорт, собранный из кода: мир пересчитывается один раз на весь модуль."""
    return build()


def test_inventory_is_up_to_date(inventory: dict):
    stored = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    assert stored == inventory, (
        "паспорт мира отстал от кода — пересоберите: make inventory."
        " Разошлись хеши дней и каталога — правили data/catalog/products.csv;"
        " разошлись только дни — правили генератор"
    )


def test_order_class_counters_cover_only_closed_days(inventory: dict):
    """Итог заказа становится опорой только после закрытия его окна."""
    rows = [row for row in inventory["days"] if "orders" in row]

    assert [row["day"] for row in rows] == [0]
    assert set(rows[0]["orders"]) == {
        "match",
        "cancelled",
        "lost_event",
        "duplicate_event",
        "amount_delta",
    }
    assert rows[0]["orders"]["lost_event"] > 0
    assert rows[0]["orders"]["duplicate_event"] > 0
    today = day_module.stream(CANONICAL_SEED, rows[0]["day"])
    assert sum(rows[0]["orders"].values()) == len(today.orders)


def test_the_inventory_holds_the_hash_of_every_sent_snapshot(inventory: dict, tmp_path):
    """У каждого отправленного слепка — своя строка с хешем его байтов.

    Байты слепка не покрыты хешами дней ни при каком раскладе подпотоков: это
    второй артефакт мира, и побайтовое обещание сторожит паспорт. Слепков на день
    меньше, чем дней: прогон дня 0 не отправляет ничего.

    Хеш сверяется с настоящей выгрузкой, а не с самим собой: в файл уезжают те
    же байты, что и в Kafka, — по строке на заказ.
    """
    days = [row["day"] for row in inventory["snapshots"]]
    assert days == list(range(STARTING_DAYS - 1))

    path = tmp_path / "snapshot.jsonl"
    assert cli.main(["snapshot", "--day", str(RUN_DAY), "--file", str(path)]) == 0
    sent = hashlib.sha256(path.read_bytes()).hexdigest()

    row = next(row for row in inventory["snapshots"] if row["day"] == RUN_DAY - 1)
    assert row["sha256"] == sent


def test_a_replayed_day_matches_its_inventory_hash(tmp_path):
    """Паспорт сверяется с байтами, вышедшими из CLI, а не с собранной тут же:
    пересчет сравнивался бы сам с собой.
    """
    stored = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    path = tmp_path / "day.jsonl"

    assert cli.main(["batch", "--day", str(REPLAY_DAY), "--file", str(path)]) == 0

    sent = hashlib.sha256(path.read_bytes()).hexdigest()
    row = next(row for row in stored["days"] if row["day"] == REPLAY_DAY)
    assert row["sha256"] == sent
