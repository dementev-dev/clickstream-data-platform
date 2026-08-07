"""Канон сериализатора: набор ключей, их порядок и форма на проводе.

Сторожится здесь то, на чём стоит сторона хранилища: строгий приём сверяет
набор ключей и разбирает даты как ISO. Разойдись сериализатор с этим — событие
уйдёт в брак целиком, а поймается это уже на стенде.
"""

import json

import pytest

from clickstream_generator import day as day_module
from clickstream_generator import schema, serialize
from clickstream_generator.seeds import CANONICAL_SEED

DAY = 2


@pytest.fixture(scope="module")
def events() -> list[dict[str, object]]:
    """События дня, разобранные обратно из канонических байтов."""
    today = day_module.stream(CANONICAL_SEED, DAY)
    return [json.loads(payload) for payload in serialize.events(today)]


def test_every_event_carries_every_column(events):
    """Все 47 ключей всегда и в порядке контракта — у любого события.

    «Пусто» по контракту — пустое значение, а не отсутствие ключа: пропавший
    ключ уводит событие в брак целиком (ADR 0005). Порядок ключей — часть
    канона: от него зависят байты, а значит и хеши манифеста.
    """
    names = [column.name for column in schema.COLUMNS]
    for event in events:
        assert list(event) == names


def test_empty_is_a_value_not_a_hole(events):
    """У просмотра страницы торговые колонки пусты, но они есть."""
    pageview = next(event for event in events if event["EventType"] == "pageview")
    assert pageview["purchaseID"] == []
    assert pageview["productPrice"] == []
    assert pageview["GoalsReached"] == []
    assert pageview["ecommerce"] == ""


def test_dates_go_as_iso(events):
    """Даты читаются глазами: `2026-06-03` и `2026-06-03T12:34:56Z`.

    Весь смысл слоя STG в том, что менти открывает колонку `raw` обычным
    клиентом и разбирает событие сам; число эпохи этот урок убивает.
    """
    for event in events[:100]:
        assert event["EventDate"] == "2026-06-03"
        assert event["UTCEventTime"].endswith("Z")
        assert len(event["UTCEventTime"]) == len("2026-06-03T12:34:56Z")


def test_ecommerce_is_a_string_with_json_inside(events):
    """`ecommerce` уезжает строкой, как отдаёт Метрика, — материал лабы."""
    purchase = next(event for event in events if event["EventType"] == "purchase")
    assert isinstance(purchase["ecommerce"], str)
    inside = json.loads(purchase["ecommerce"])
    assert inside["purchase"]["actionField"]["id"] == purchase["purchaseID"][0]


def test_limit_takes_the_beginning_of_the_day(events):
    """Ограниченная пачка — начало дня, а не его пересборка другими байтами."""
    today = day_module.stream(CANONICAL_SEED, DAY)
    short = serialize.events(today, limit=10)
    assert len(short) == 10
    assert [json.loads(payload) for payload in short] == events[:10]
