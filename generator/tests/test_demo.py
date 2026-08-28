"""Запускаемый путеводитель остается коротким и проходит всю цепочку."""

import subprocess
from pathlib import Path


def test_demo_runs_and_shows_the_whole_chain() -> None:
    """Сторожит команду и стадии, а числа мира оставляет описи."""
    finished = subprocess.run(
        ["make", "--silent", "demo"],
        check=False,
        capture_output=True,
        cwd=Path(__file__).parents[1],
        text=True,
    )

    assert finished.returncode == 0, finished.stderr
    lines = finished.stdout.splitlines()
    assert [line.partition(":")[0] for line in lines] == [
        "Зерно",
        "Подпотоки D0",
        "Когорта D0",
        "Аудитория D0",
        "Визиты по VisitID",
        "Строки событий",
    ]
    assert "когорты D-" in lines[3] and "...D0" in lines[3]
    assert "строк в группе" in lines[4]
    assert all(
        label in lines[5]
        for label in ("pageview", "add_to_cart", "purchase", "заказов")
    )
