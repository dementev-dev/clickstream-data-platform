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

**Слепки заказов идут третьим ходом того же проигрывателя** и живут по правилу
сдвига: прогон дня D отправляет слепок дня D−1 — ночная выгрузка бэкенда за
вчера. Правило записано здесь одно и целиком, потому что оно и есть разница
между двумя источниками: трекер шлёт сегодняшний день, бэкенд — вчерашний.
"""

import logging
import time
from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from clickstream_generator import day as day_module
from clickstream_generator import orders as orders_module
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


def play_snapshots(sink: Sink, seed: int, first_day: int, days: int = 1) -> None:
    """Отправить слепки прогонов `days` дней подряд начиная с `first_day`.

    Прогон дня D отправляет слепок дня D−1: содержимое слепка — чистая функция
    зерна и дня, от момента отправки оно не зависит, а сдвиг делает живой день
    обычным. На старте оси вчера нет, поэтому прогон дня 0 не отправляет
    ничего.

    Слепок собирается переигровкой дней своего окна: заказы дня — производная
    всей воронки, дешёвого пути к ним нет. Окна соседних слепков перекрываются
    почти целиком, и внутри одного прогона день играется один раз — иначе
    стартовый диапазон стоил бы полусотни проигрышей вместо восьми. Между
    прогонами не остаётся ничего: кэш на томе был бы состоянием, которого у
    проигрывателя нет.
    """
    played: dict[int, orders_module.Orders] = {}
    sent = 0
    generated = 0.0
    delivered = 0.0

    for number in range(first_day, first_day + days):
        taken = number - 1
        if taken < 0:
            continue

        clock = time.monotonic()
        window = [
            _orders_of(played, seed, born) for born in orders_module.window(taken)
        ]
        payloads = serialize.orders(window, taken)
        spent = time.monotonic() - clock
        generated += spent
        log.info(
            "слепок дня %d: заказов %d, генерация %.1f с", taken, len(payloads), spent
        )

        clock = time.monotonic()
        _send(sink, payloads)
        sink.flush()
        spent = time.monotonic() - clock
        delivered += spent

        sent += len(payloads)
        log.info(
            "слепок дня %d: отправлено %d, доставка %.1f с", taken, len(payloads), spent
        )

    log.info(
        "итого отправлено %d заказов: генерация %.1f с, доставка %.1f с",
        sent,
        generated,
        delivered,
    )


def _orders_of(
    played: dict[int, orders_module.Orders], seed: int, number: int
) -> orders_module.Orders:
    """Заказы дня `number`, сыгранного один раз на весь прогон."""
    if number not in played:
        played[number] = day_module.stream(seed, number).orders
    return played[number]


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
