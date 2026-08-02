"""Идентификаторы событий: неповторяющиеся и переживающие JSON.

Обещание одно, но держит его дедупликация при переигровке дня: два
одинаковых `WatchID` склеили бы разные события (спека, раздел 4).
"""

import numpy as np

from clickstream_generator import ids
from clickstream_generator.seeds import CANONICAL_SEED


def stream(offset: int = 0) -> np.random.Generator:
    return np.random.Generator(np.random.PCG64(CANONICAL_SEED + offset))


def test_ids_do_not_repeat_and_survive_json():
    numbers = ids.unique(stream(), 10_000)
    assert numbers.dtype == np.uint64
    assert len(set(numbers.tolist())) == numbers.size
    assert numbers.max() < ids.LIMIT
    assert numbers.min() > 0


def test_ids_do_not_give_away_the_order_of_the_rows():
    """Иначе номер события рассказывал бы, каким по счёту оно родилось."""
    numbers = ids.unique(stream(), 1_000)
    assert not np.all(np.diff(numbers.astype(np.int64)) > 0)


def test_ids_of_another_stream_are_other_ids():
    assert not np.array_equal(ids.unique(stream(), 100), ids.unique(stream(1), 100))


def test_ids_can_be_drawn_beside_the_ones_already_taken():
    """Торговые строки берут номера из своего подпотока и не задевают чужие."""
    taken = ids.unique(stream(), 5_000)
    numbers = ids.unique_apart_from(stream(1), 500, taken)
    assert len(set(numbers.tolist())) == numbers.size
    assert not set(numbers.tolist()) & set(taken.tolist())
    assert numbers.max() < ids.LIMIT
