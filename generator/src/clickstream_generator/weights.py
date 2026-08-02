"""Выбор по целым весам — общий приём плана состава и дня-функции.

Дисциплина спеки (раздел 2): случайность тянется целыми числами, плавающие
распределения системной математики не зовутся — они расходятся между
версиями numpy и архитектурами CPU, а обещано побайтовое совпадение. Отсюда
и способ: веса складываются в накопленный ряд, бросок попадает в чью-то долю
общего веса, и кто долю занимал — тот и выбран.

Модуль маленький нарочно: у приёма одно определение на весь генератор,
иначе дисциплина живёт копиями и расходится с ними.
"""

import numpy as np
from numpy.typing import NDArray


def pick(
    rng: np.random.Generator, cumulative: NDArray[np.int64], size: int
) -> NDArray[np.int64]:
    """Куда попал бросок в общий вес — тот вариант и вышел."""
    return np.searchsorted(cumulative, rng.integers(0, cumulative[-1], size), "right")


def pick_row(
    rng: np.random.Generator, cumulative: NDArray[np.int64], rows: NDArray[np.int64]
) -> NDArray[np.int64]:
    """То же, но у каждого броска своя строка таблицы весов.

    Строки уложены встык — каждая начинается там, где кончилась предыдущая, —
    и поиск идёт по одному ряду: столько же работы, сколько на одну таблицу.
    Общий вес у строк поэтому обязан совпадать; у повёрнутой суточной волны
    он совпадает по построению.
    """
    total = cumulative[0, -1]
    shelf = (cumulative + np.arange(cumulative.shape[0])[:, None] * total).ravel()
    draw = rows * total + rng.integers(0, total, rows.size)
    return np.searchsorted(shelf, draw, "right") - rows * cumulative.shape[1]
