"""Проигрыватель: гонит дни мира в приёмник — пачкой или с темпом живого дня.

Состояния у него нет (спека генератора, раздел 9). Зерно и номер дня приходят
параметрами, позицию на оси он не хранит и из данных не выводит: её ведёт тот,
кто зовёт, — на этапе 5 это переменная Airflow у дага `next_day`. Отсюда и
переигровка обрыва: позвали тот же день заново — получили те же `WatchID`, и
дедупликация склеила повтор.

Два режима отличаются только темпом. Пакетный шлёт события подряд, без пауз, —
это заливка снимка и переигровка дня. Живой держит модельное время: событие
уезжает тогда, когда до него дошли модельные часы, поделённые на ускорение.
По умолчанию ускорение ×60 — модельные сутки за 24 реальные минуты (спека,
раздел 5): суточная волна разворачивается на глазах.

Тайминги печатаются раздельно — генерация и доставка, как требует спека
(раздел 5): это разные машины разной природы, и сложенные в одно число они
перестают что-либо говорить. Сериализация считается частью генерации: она
рождает те самые байты, которые сторожит опись.
"""

import logging
import time
from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from clickstream_generator import day as day_module
from clickstream_generator import serialize
from clickstream_generator.sinks import Sink

log = logging.getLogger(__name__)

# Как часто живой режим отчитывается о ходе дня. Минута реального времени — это
# час модельного при ×60: отчёт на каждый модельный час.
REPORT_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class Played:
    """Итог прогона: сколько уехало и за сколько."""

    events: int
    generated_seconds: float
    delivered_seconds: float


def play(
    sink: Sink,
    seed: int,
    first_day: int,
    days: int = 1,
    limit: int | None = None,
    speed: float | None = None,
) -> Played:
    """Проиграть `days` дней подряд начиная с `first_day` в приёмник `sink`.

    `limit` — потолок событий на весь прогон: срез для того, кто смотрит на
    конвейер и не хочет ждать целый день. `speed` — ускорение живого режима;
    `None` означает пакетный, то есть без пауз вовсе.
    """
    events = 0
    generated = 0.0
    delivered = 0.0

    for number in range(first_day, first_day + days):
        left = None if limit is None else limit - events
        if left is not None and left <= 0:
            break

        clock = time.monotonic()
        today = day_module.stream(seed, number)
        payloads = serialize.events(today, limit=left)
        seconds = _event_seconds(today, len(payloads))
        spent = time.monotonic() - clock
        generated += spent
        log.info("день %d: событий %d, генерация %.1f с", number, len(payloads), spent)

        clock = time.monotonic()
        if speed is None:
            _send(sink, payloads)
        else:
            _send_paced(sink, payloads, seconds, speed)
        # Рубеж дня: доставка асинхронна, и без него напечатанное время
        # означало бы только «события легли в очередь отправителя».
        sink.flush()
        spent = time.monotonic() - clock
        delivered += spent

        events += len(payloads)
        log.info(
            "день %d: отправлено %d, доставка %.1f с", number, len(payloads), spent
        )

    log.info(
        "итого отправлено %d событий: генерация %.1f с, доставка %.1f с",
        events,
        generated,
        delivered,
    )
    return Played(
        events=events, generated_seconds=generated, delivered_seconds=delivered
    )


def _send(sink: Sink, payloads: list[bytes]) -> None:
    """Пакетно: подряд и без пауз."""
    for payload in payloads:
        sink.send(payload)


def _send_paced(
    sink: Sink, payloads: list[bytes], seconds: NDArray[np.int64], speed: float
) -> None:
    """С темпом: событие уезжает, когда до него дошло модельное время.

    Отставание не догоняется рывком и не прячется: спешить некуда — событие
    всё равно уедет, — а вот увидеть отставание в логе нужно, иначе живой
    режим врёт про темп. Обгонять модельное время нельзя, отставать можно, и
    именно это печатает отчёт.
    """
    started = time.monotonic()
    origin = int(seconds[0]) if seconds.size else 0
    reported = started
    lag = 0.0

    for number, payload in enumerate(payloads):
        due = started + (int(seconds[number]) - origin) / speed
        now = time.monotonic()
        if now < due:
            time.sleep(due - now)
        else:
            lag = max(lag, now - due)
        sink.send(payload)

        now = time.monotonic()
        if now - reported >= REPORT_SECONDS:
            log.info(
                "проиграно %d из %d, модельное время %s, лаг %.1f с",
                number + 1,
                len(payloads),
                _model_time(int(seconds[number]) - origin),
                lag,
            )
            reported = now
            lag = 0.0


def _event_seconds(today: day_module.Day, count: int) -> NDArray[np.int64]:
    """Секунды событий абсолютной меткой — по ним живой режим держит темп."""
    times: NDArray[np.datetime64] = today.columns["UTCEventTime"][:count]
    return times.astype("datetime64[s]").astype(np.int64)


def _model_time(elapsed: int) -> str:
    """Прожитое модельное время дня в виде `ЧЧ:ММ` — от первого события."""
    return f"{elapsed // 3600:02d}:{elapsed % 3600 // 60:02d}"
