"""Иерархия зёрен: откуда любая часть мира берёт свою случайность.

Дерево подпотоков (спека генератора, раздел 2):

    корневое зерно
    ├── состав мира
    │   ├── ось          → номер дня: когорта этого дня
    │   └── предыстория  → глубина: когорта дня до D0
    └── дни
        └── номер дня    → трафик, торговля, заказы, маячки счетчика

Механизм — `numpy.random.SeedSequence`: потомок полностью определяется парой
(зерно, позиция в дереве), а не порядком вычислений. Сверено через Context7
по документации numpy (2026-08-01) и проверено тестом: `spawn_key`, выписанный
руками, даёт тот же подпоток, что цепочка `spawn`. На этом держатся три
обещания: параллельный прогон равен последовательному, день N+1 не трогает
дни 1…N, правка одного компонента меняет только его часть снимка.

Позиция в дереве — неотрицательные целые, а дни предыстории отрицательны;
поэтому у предыстории своя ветвь, а не общий ряд с осью.
"""

from collections.abc import Sequence
from enum import IntEnum

import numpy as np

# Каноническое зерно эталонного мира — константа репозитория; опись хранит
# его в паспорте мира. Свои зёрна менти крутит без гарантий описи.
CANONICAL_SEED = 20260601


class Component(IntEnum):
    """Подпотоки внутри дня; порядок объявления — позиция в дереве."""

    TRAFFIC = 0
    COMMERCE = 1
    # Заказная сторона: деньги магазина и судьба заказа. Ветвится по дню
    # рождения заказа — слепок несёт семь дней рождения сразу, и судьбу
    # каждого заказа читает из его собственного дня
    # (docs/architecture/orders/fate.md).
    ORDERS = 2
    # Маячки счетчика: потеря и повторная отправка событий.
    BEACONS = 3


class _Branch(IntEnum):
    """Две ветви корня: постоянный состав мира и проживание дней."""

    COMPOSITION = 0
    DAYS = 1


class _Era(IntEnum):
    """Две ветви состава: ось событий и предыстория до D0."""

    AXIS = 0
    PREHISTORY = 1


def _stream(seed: int, position: Sequence[int]) -> np.random.Generator:
    """Подпоток на позиции `position` дерева зерна `seed`."""
    sequence = np.random.SeedSequence(entropy=seed, spawn_key=position)
    return np.random.Generator(np.random.PCG64(sequence))


def cohort_stream(seed: int, day: int) -> np.random.Generator:
    """Случайность когорты дня `day`; отрицательный день — предыстория."""
    era, index = (_Era.AXIS, day) if day >= 0 else (_Era.PREHISTORY, -day - 1)
    return _stream(seed, (_Branch.COMPOSITION, era, index))


def day_stream(seed: int, day: int, component: Component) -> np.random.Generator:
    """Случайность одного компонента дня `day` — дня оси, не предыстории."""
    if day < 0:
        raise ValueError(f"события начинаются в D0: дня {day} на оси нет")
    return _stream(seed, (_Branch.DAYS, day, component))
