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
    """Дни до D0 отрицательны, позиция в дереве — нет: своя ветвь."""
    for depth in range(1, 5):
        assert first_draws(cohort_stream(CANONICAL_SEED, -depth)) != first_draws(
            cohort_stream(CANONICAL_SEED, depth)
        )


def test_day_components_do_not_share_randomness():
    draws = [
        first_draws(day_stream(CANONICAL_SEED, 3, component)) for component in Component
    ]
    assert len({tuple(draw) for draw in draws}) == len(Component)


def test_composition_and_day_are_separate_streams():
    """Состав мира ветвится сам по себе — день-функция его не сдвигает."""
    assert first_draws(cohort_stream(CANONICAL_SEED, 3)) != first_draws(
        day_stream(CANONICAL_SEED, 3, Component.TRAFFIC)
    )


def test_another_seed_is_another_world():
    assert first_draws(cohort_stream(CANONICAL_SEED, 3)) != first_draws(
        cohort_stream(CANONICAL_SEED + 1, 3)
    )
