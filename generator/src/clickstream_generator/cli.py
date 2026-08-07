"""Интерфейс запуска: `python -m clickstream_generator batch|live`.

Зовущий — контейнер (спека генератора, раздел 9), и интерфейс сделан под него:
параметры приходят аргументами или переменными окружения, логи идут в
стандартный вывод, итог виден кодом возврата. Ни файла настроек, ни состояния
между запусками нет — позиция на оси принадлежит тому, кто зовёт.

Зовущих трое, и все трое видны в форме команд:

- даги `world_init` и `next_day` этапа 5 — по дню за запуск, приёмник Kafka;
- заливка стартового мира — восемь дней подряд одним запуском: `--days`;
- проверки хранилища (#43) — ограниченная пачка в файл: `--limit` и `--file`.

**Режимы разведены командами, а не флагом**, потому что различаются не темпом
в числе, а тем, что у них разное: у пакетного есть `--limit` и нет ожидания, у
живого есть `--speed` и нет пачки. Один флаг `--speed 0` прятал бы это
различие за числом, а команда называет его словом. Ограниченная пачка живому
дню не полагается: ждать там нечего — ожидание снимает ускорение.

**Приёмник выбирается тем, что для него назвали**: `--file` или `--brokers`.
Оба сразу — ошибка, ни одного — тоже: молча выбранный по умолчанию приёмник
однажды напишет в файл то, чего ждали в Kafka.

**Умолчания есть только у того, что описывает мир, а не окружение.** Зерно,
число дней и темп живого дня — свойства модели, они заданы спекой и тикетом,
и умолчание здесь — тот же самый ответ. Позиция на оси (`--day`), адрес
брокера, имя топика и путь файла — факты стенда, на котором нас запустили:
генератор их не знает и знать не должен, он отдельная и переносимая сущность.
Подставь он своё умолчание — забытый параметр превратился бы в неверный ответ,
поданный как успех.
"""

import argparse
import logging
import os
import sys
from collections.abc import Callable, Sequence
from contextlib import closing
from pathlib import Path

from clickstream_generator import player
from clickstream_generator.seeds import CANONICAL_SEED
from clickstream_generator.sinks import FileSink, KafkaSink, Sink

DEFAULT_SPEED = 60.0


def main(argv: Sequence[str] | None = None) -> int:
    """Разобрать аргументы, проиграть дни, вернуть код возврата."""
    parser = _parser()
    options = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(message)s", stream=sys.stdout, force=True
    )

    _check(parser, options)
    try:
        with closing(_sink(parser, options)) as sink:
            player.play(
                sink,
                seed=options.seed,
                first_day=options.day,
                days=options.days,
                limit=options.limit,
                speed=options.speed,
            )
    except (OSError, RuntimeError) as failure:
        logging.error("прогон не удался: %s", failure)
        return 1
    return 0


def _parser() -> argparse.ArgumentParser:
    """Разбор командной строки; умолчания приходят из окружения."""
    parser = argparse.ArgumentParser(
        prog="python -m clickstream_generator",
        description="Проигрыватель модельных дней кликстрима в файл или Kafka.",
    )
    modes = parser.add_subparsers(dest="mode", required=True)

    batch = modes.add_parser(
        "batch", help="пачкой, без пауз: заливка снимка и переигровка дня"
    )
    _common(batch)
    batch.set_defaults(speed=None)
    batch.add_argument(
        "--limit",
        type=int,
        default=_env_int("GENERATOR_LIMIT", None),
        metavar="N",
        help="взять не больше N событий на весь прогон, а не день целиком",
    )

    live = modes.add_parser("live", help="живой день: темп модельного времени")
    _common(live)
    live.set_defaults(limit=None)
    live.add_argument(
        "--speed",
        type=float,
        default=_env_float("GENERATOR_SPEED", DEFAULT_SPEED),
        metavar="X",
        help=f"ускорение модельного времени (по умолчанию ×{DEFAULT_SPEED:.0f})",
    )
    return parser


def _common(parser: argparse.ArgumentParser) -> None:
    """Что спрашивают у обоих режимов: какой мир, какие дни и куда."""
    parser.add_argument(
        "--seed",
        type=int,
        default=_env_int("GENERATOR_SEED", CANONICAL_SEED),
        metavar="N",
        help="зерно мира (по умолчанию каноническое)",
    )
    parser.add_argument(
        "--day",
        type=int,
        default=_env_int("GENERATOR_DAY", None),
        metavar="D",
        help="номер дня на оси мира (D0 — первый); умолчания нет —"
        " позицию ведёт зовущий",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=_env_int("GENERATOR_DAYS", 1),
        metavar="N",
        help="сколько дней подряд проиграть одним запуском",
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=_env("GENERATOR_FILE", None, Path),
        metavar="ПУТЬ",
        help="приёмник — файл: одно событие в строке",
    )
    parser.add_argument(
        "--brokers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        metavar="АДРЕС",
        help="приёмник — Kafka: адреса брокеров через запятую",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC"),
        metavar="ИМЯ",
        help="топик Kafka; умолчания нет — имя топика принадлежит стенду, а не пакету",
    )


def _check(parser: argparse.ArgumentParser, options: argparse.Namespace) -> None:
    """День на оси обязан быть назван — иначе прогон не начинается."""
    if options.day is None:
        parser.error(
            "день на оси не назван: передайте --day или GENERATOR_DAY."
            " Позицию ведёт зовущий — проигрыватель её не помнит"
        )


def _sink(parser: argparse.ArgumentParser, options: argparse.Namespace) -> Sink:
    """Приёмник по названному: файл либо Kafka, но не оба и не ничего."""
    if options.file and options.brokers:
        parser.error("названы оба приёмника: оставьте --file либо --brokers")
    if options.file:
        return FileSink(options.file)
    if options.brokers:
        if not options.topic:
            parser.error(
                "топик не назван: передайте --topic или KAFKA_TOPIC."
                " Имя топика — факт стенда, генератор его не знает"
            )
        try:
            return KafkaSink(options.brokers, options.topic)
        except ImportError:
            # Ловим там, где возникает: обёрнутый вокруг всего прогона,
            # этот перехват однажды объявил бы «нет клиента Kafka» о чужой
            # сорванной загрузке модуля.
            parser.error(
                "приёмник Kafka требует клиента: поставьте пакет с группой"
                " зависимостей kafka (`uv sync --extra kafka`)"
            )
    parser.error("приёмник не назван: нужен --file либо --brokers")


def _env[T](name: str, fallback: T, kind: Callable[[str], T]) -> T:
    """Значение переменной окружения нужного типа; нет переменной — умолчание.

    Пустая строка считается отсутствием: в compose так выглядит переменная,
    которую не задали, — `${GENERATOR_DAYS:-}`. Мусор в переменной называется
    вместе с её именем: из контейнера иначе не видно, чьё это значение.
    """
    value = os.environ.get(name)
    if not value:
        return fallback
    try:
        return kind(value)
    except ValueError:
        raise SystemExit(f"переменная {name} не разбирается: {value!r}") from None


def _env_int(name: str, fallback: int | None) -> int | None:
    return _env(name, fallback, int)


def _env_float(name: str, fallback: float) -> float:
    return _env(name, fallback, float)
