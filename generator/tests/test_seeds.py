"""Свойства иерархии зёрен, на которых держится детерминизм.

Проверяется не «числа такие-то» (они зависят от numpy и закреплены
`uv.lock`), а три обещания спеки, раздел 2: подпоток определяется позицией
в дереве, а не порядком вычислений; продление истории днём N+1 не трогает
дни 1…N; правка одного компонента не задевает соседние.
"""

import numpy as np

from clickstream_generator.seeds import (
    CANONICAL_SEED,
    Component,
    cohort_stream,
    day_stream,
)


def first_draws(stream: np.random.Generator) -> list[int]:
    """Отпечаток подпотока: первые броски целыми."""
    return stream.integers(0, 2**32, 8).tolist()


def test_canonical_seed_is_a_repository_constant():
    assert isinstance(CANONICAL_SEED, int)


def test_addressing_a_subtree_equals_spawning_down_to_it():
    """Свойство numpy, на котором стоит вся раздача зерна.

    Потомок определяется парой (зерно, позиция в дереве): выписанный руками
    `spawn_key` даёт тот же подпоток, что цепочка `spawn`. Иначе результат
    зависел бы от порядка вычислений, и «параллельно равно последовательно»
    не выполнялось бы.
    """
    chained = np.random.SeedSequence(CANONICAL_SEED).spawn(1)[0].spawn(4)[3]
    addressed = np.random.SeedSequence(CANONICAL_SEED, spawn_key=(0, 3))
    assert chained.spawn_key == addressed.spawn_key
    assert first_draws(np.random.Generator(np.random.PCG64(chained))) == first_draws(
        np.random.Generator(np.random.PCG64(addressed))
    )


def test_stream_is_a_position_in_the_tree_not_an_order_of_calls():
    straight = first_draws(cohort_stream(CANONICAL_SEED, 5))
    cohort_stream(CANONICAL_SEED, 0)
    day_stream(CANONICAL_SEED, 3, Component.TRAFFIC)
    detoured = first_draws(cohort_stream(CANONICAL_SEED, 5))
    assert straight == detoured


def test_cohorts_of_different_days_are_independent():
    draws = [first_draws(cohort_stream(CANONICAL_SEED, day)) for day in range(5)]
    assert len({tuple(draw) for draw in draws}) == len(draws)


def test_prehistory_does_not_collide_with_the_axis():
    """Дни до D0 отрицательны, позиция в дереве — нет: своя ветвь.

    Сравнивать день −N с днём N мало: слейся эти ветви, столкнулись бы −N
    и N−1 — глубину предыстория считает от единицы, а ось дни от нуля.
    Поэтому каждый день предыстории сверяется со всем началом оси.
    """
    axis = {tuple(first_draws(cohort_stream(CANONICAL_SEED, day))) for day in range(6)}
    for depth in range(1, 6):
        prehistoric = tuple(first_draws(cohort_stream(CANONICAL_SEED, -depth)))
        assert prehistoric not in axis


def test_day_components_do_not_share_randomness():
    """Четыре подпотока дня из спеки — и они четыре разных.

    Компоненты перечислены поимённо, а не обходом `Component`: слейся два
    имени в одно значение, обход молча стал бы короче, и тест сверял бы
    сам себя.
    """
    components = (
        Component.TRAFFIC,
        Component.COMMERCE,
        Component.ORDERS,
        Component.LATECOMERS,
    )
    assert len({int(component) for component in components}) == 4
    draws = {
        tuple(first_draws(day_stream(CANONICAL_SEED, 3, component)))
        for component in components
    }
    assert len(draws) == 4


def test_composition_and_day_never_share_a_stream():
    """Состав мира ветвится сам по себе — день-функция его не сдвигает.

    Сверять день N с составом дня N мало: слейся эти ветви, столкнулись бы
    состав дня K и K-й компонент дня 0 — номер дня в одном адресе стоит
    там же, где номер компонента в другом. Поэтому каждый компонент
    сверяется со всем куском состава, куда он мог бы попасть.
    """
    composition = {
        tuple(first_draws(cohort_stream(CANONICAL_SEED, day))) for day in range(-5, 6)
    }
    for day in range(5):
        for component in Component:
            stream = first_draws(day_stream(CANONICAL_SEED, day, component))
            assert tuple(stream) not in composition


def test_another_seed_is_another_world():
    assert first_draws(cohort_stream(CANONICAL_SEED, 3)) != first_draws(
        cohort_stream(CANONICAL_SEED + 1, 3)
    )
