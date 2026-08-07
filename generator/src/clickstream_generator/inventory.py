"""Опись мира: чем стенд наполняется при подъёме и каким это обязано выйти.

Мир — чистая функция зерна (спека генератора, раздел 2), поэтому в git лежит не
он сам, а опись: паспорт мира, число событий по дням и хеш байтов каждого дня.
Сам мир пересчитывается когда угодно, а опись отвечает на единственный вопрос —
**тот ли это мир, что был вчера**. Разошлись хеши — мир уехал, и дальше уже
неважно, чего от него ждали проверки.

Дней в описи восемь: столько заливается в стенд при `make up`. Понедельник по
понедельник — полная неделя с выходными и первый замкнутый цикл окна K = 7.
Эталонный снимок в четырнадцать дней придёт на этапе 7 и станет продолжением
этой же описи, а не вторым файлом.

**Сторожат мир хеши, а не паспорт.** Паспорт отвечает на другой вопрос — «чем
это сделано»: зерно и версия генератора. Поменяй кто-нибудь код так, что мир
сдвинется, — версия останется прежней, а хеши покраснеют; наоборот не бывает.

Хеш каталога стоит здесь по третьему основанию — ни сторожить, ни описывать, а
**объяснять**. Правка цены в `data/catalog/products.csv` меняет мир так же
молча, как правка кода, и по одним хешам эти два случая неразличимы. С хешем
каталога различимы: разошлись хеши дней и каталога — правили CSV; разошлись
только дни — правили код.

Хеш дня — sha256 тех самых байтов, что уезжают в Kafka, с переводом строки
после каждого события. Это ровно то, что пишет файловый приёмник, поэтому
пересчитывается он и обычным `sha256sum` по сыгранному в файл дню (как
именно — в README репозитория).

Собирается опись из корня репозитория целью `make inventory`, а свежесть её
сторожит тест — как и у «описания выгрузки».
"""

import argparse
import hashlib
import json
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any

from clickstream_generator import day as day_module
from clickstream_generator import serialize, world
from clickstream_generator.catalog import CATALOG_PATH
from clickstream_generator.seeds import CANONICAL_SEED

# Сколько дней оси заливается в стенд при подъёме. То же число стоит у службы
# `world-init` в compose.yaml: YAML не читает Python, и одно из двух мест —
# лишнее по построению. Расхождение поймают счётчики make check-clickhouse.
STARTING_DAYS = 8


def build() -> dict[str, Any]:
    """Опись целиком: паспорт мира и по строке на каждый его день."""
    return {
        "seed": CANONICAL_SEED,
        "generator_version": version("clickstream-generator"),
        "catalog_sha256": _digest(CATALOG_PATH.read_bytes()),
        "days": [_day(number) for number in range(STARTING_DAYS)],
    }


def render() -> str:
    """Опись текстом файла: отступы в два пробела, кириллица как есть."""
    return json.dumps(build(), ensure_ascii=False, indent=2) + "\n"


def _day(number: int) -> dict[str, Any]:
    """Строка описи: номер дня, его дата, число событий и хеш байтов.

    Дата считается от D0 арифметикой, а не берётся из событий: ось модельного
    времени так и определена (`world.ORIGIN`), и по этой же дате счётчики
    стенда обрамляют счёт в `ods.event`. Соври она — подневная сверка это и
    покажет, каждый день сразу.
    """
    payloads = serialize.events(day_module.stream(CANONICAL_SEED, number))
    return {
        "day": number,
        "date": (world.ORIGIN + timedelta(days=number)).isoformat(),
        "events": len(payloads),
        "sha256": _digest(b"".join(payload + b"\n" for payload in payloads)),
    }


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Собирает опись мира: паспорт, счётчики и хеши дней."
    )
    parser.add_argument("output", type=Path, help="путь к файлу описи")
    output = parser.parse_args().output
    output.write_text(render(), encoding="utf-8")
    print(f"Опись мира собрана: {output}")


if __name__ == "__main__":
    main()
