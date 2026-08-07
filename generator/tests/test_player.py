"""Проигрыватель и его интерфейс: побайтовый повтор, построчность, приёмник.

Главное здесь — обещание раздела 2 спеки, доведённое до байтов на диске: два
прогона одного дня дают тот же файл. Тем же тестом сторожится построчность:
одно событие — одна строка. Дешёвая половина контракта транспорта — склейка
двух событий сломала бы разбор целиком, потому что хранилище читает топик
байтами.
"""

import hashlib
import subprocess
import sys
from contextlib import closing
from dataclasses import replace

import pytest

from clickstream_generator import cli, player
from clickstream_generator import day as day_module
from clickstream_generator.seeds import CANONICAL_SEED
from clickstream_generator.sinks import FileSink

DAY = 2

# Переменные, которыми зовущий задаёт прогон. Тест, читающий их из окружения
# машины, зелен у одного и красен у другого — а на этой машине они как раз и
# живут: стенд их экспортирует.
LAUNCH_VARIABLES = (
    "GENERATOR_SEED",
    "GENERATOR_DAY",
    "GENERATOR_DAYS",
    "GENERATOR_LIMIT",
    "GENERATOR_SPEED",
    "GENERATOR_FILE",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
)


@pytest.fixture(autouse=True)
def bare_environment(monkeypatch):
    """Прогон тестов не зависит от того, что задано в окружении машины."""
    for name in LAUNCH_VARIABLES:
        monkeypatch.delenv(name, raising=False)


def _play(path, **options):
    """Проиграть в файл и вернуть итог прогона."""
    with closing(FileSink(path)) as sink:
        return player.play(sink, seed=CANONICAL_SEED, first_day=DAY, **options)


def test_two_runs_give_the_same_file(tmp_path):
    """Два прогона дня — одинаковые байты и строка на событие.

    Хеши сравниваются целиком, а не построчно: обещание побайтовое, и
    расхождение в одной запятой обязано покраснеть так же, как расхождение в
    наборе событий.

    Прогоны идут **разными процессами**, а не двумя вызовами в одном. Внутри
    одного интерпретатора зерно хеширования общее на оба прогона, поэтому
    зависимость канона от порядка обхода множества такой тест не увидел бы
    никогда — а это ровно тот класс расхождений, ради которого обещание и
    дано. Заодно день играется тем же путём, каким генератор зовут на самом
    деле: через интерфейс запуска, а не через функцию.
    """
    first, second = tmp_path / "first.jsonl", tmp_path / "second.jsonl"
    _run_apart(first)
    _run_apart(second)

    assert _digest(first) == _digest(second)

    events = len(day_module.stream(CANONICAL_SEED, DAY))
    assert len(first.read_bytes().splitlines()) == events


def _run_apart(path) -> None:
    """Проиграть день отдельным процессом; окружение он берёт от нас."""
    finished = subprocess.run(
        [
            sys.executable,
            "-m",
            "clickstream_generator",
            "batch",
            "--day",
            str(DAY),
            "--file",
            str(path),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert finished.returncode == 0, finished.stderr


def test_days_play_in_a_row(tmp_path, monkeypatch):
    """Дни идут подряд от названного, а пачка считается на весь прогон.

    Восемь дней одним запуском — то, чем зальётся зерновой мир (#42), поэтому
    порядок дней проверяется, а не предполагается. День здесь подменён коротким:
    проверяется ход проигрывателя, а не содержимое дня, и платить за полсотни
    тысяч событий трижды незачем.
    """
    short = _shorten(day_module.stream(CANONICAL_SEED, DAY), rows=2)
    asked: list[int] = []

    def stream(seed: int, number: int):
        asked.append(number)
        return short

    monkeypatch.setattr(player.day_module, "stream", stream)
    path = tmp_path / "three-days.jsonl"
    played = _play(path, days=3, limit=5)

    assert asked == [DAY, DAY + 1, DAY + 2]
    # Двум дням хватило по два события, третьему досталось последнее: потолок
    # считается на весь прогон, а не на каждый день заново.
    assert played.events == 5
    assert len(path.read_bytes().splitlines()) == 5


FILE = ["--file", "/dev/null"]
KAFKA = ["--brokers", "kafka:29092", "--topic", "hits"]


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["batch", "--day", "0"], id="приёмник не назван"),
        pytest.param(["batch", "--day", "0", *FILE, *KAFKA], id="названы оба"),
        pytest.param(
            ["batch", "--day", "0", "--brokers", "kafka:29092"], id="топик не назван"
        ),
        pytest.param(["batch", *FILE], id="день не назван"),
    ],
)
def test_run_is_refused_loudly(argv):
    """Всё, чего проигрыватель не знает, — отказ до первого события.

    Правило одно: параметр, описывающий окружение стенда или позицию на оси
    мира, своего умолчания не имеет. Тихо подставленное умолчание — чужой факт,
    выданный за наш, и прогон отчитается о нём успехом.
    """
    with pytest.raises(SystemExit) as refusal:
        cli.main(argv)
    assert refusal.value.code == 2


def test_cli_plays_a_limited_batch_into_a_file(tmp_path):
    """Тот самый вызов, которым проверки #43 берут срез: код возврата — 0."""
    path = tmp_path / "cli.jsonl"
    assert cli.main(["batch", "--day", "0", "--limit", "5", "--file", str(path)]) == 0
    assert len(path.read_bytes().splitlines()) == 5


def _shorten(today: day_module.Day, rows: int) -> day_module.Day:
    """Тот же день, но короткий: первые строки всех его рядов."""
    return replace(
        today,
        columns={name: value[:rows] for name, value in today.columns.items()},
        page=today.page[:rows],
        product=today.product[:rows],
    )


def _digest(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
