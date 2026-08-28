"""Слепок заказов: окно, граница суток, запись на проводе и байты прогона.

Слепок — второй артефакт мира, и сторожится он тем же, чем день событий:
формой записи и побайтовым повтором. Проверяются пять обещаний
(docs/architecture/orders/snapshot.md):

1. **Окно.** Слепок дня D несёт заказы, рождённые в дни D−6…D, и у начала оси
   усекается сам.
2. **Граница суток.** Состояние — чтение готовой судьбы на границе D|D+1:
   моменты позже границы не учитываются, поэтому заказ «дышит» — `created` в
   одном слепке, `paid` в следующем.
3. **Запись на проводе.** Одиннадцать ключей в порядке контракта, деньги
   строками с двумя знаками, времена RFC 3339 с миллисекундами.
4. **Сдвиг отправки.** Прогон дня D отправляет слепок дня D−1; прогон дня 0 —
   ничего.
5. **Повтор.** Пересъёмка слепка даёт те же байты.

День рождения заказа берётся из его номера, а не из `created_at`: номер
считается в поясе счётчика, а `created_at` — абсолютная метка, и у ночной
покупки их даты расходятся.
"""

import hashlib
import json
import re
import subprocess
import sys
from contextlib import closing
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import numpy as np
import pytest

from clickstream_generator import catalog, cli, commerce, player, serialize, world
from clickstream_generator import day as day_module
from clickstream_generator.seeds import CANONICAL_SEED
from clickstream_generator.sinks import FileSink

# Первый слепок с полным окном и слепок у начала оси, где окно усечено.
FULL_WINDOW_DAY = world.ORDER_WINDOW_DAYS - 1
SHORT_WINDOW_DAY = 3

# Порядок ключей записи — порядок полей контракта (мастер-спека, раздел 2).
CONTRACT = (
    "order_id",
    "user_id",
    "status",
    "created_at",
    "updated_at",
    "items_total",
    "discount",
    "delivery",
    "total",
    "items",
    "snapshot_date",
)
ITEM = ("sku", "qty", "price")
STATUSES = {"created", "paid", "cancelled"}
MONEY = re.compile(r"\d+\.\d{2}")
MOMENT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")


@pytest.fixture(scope="module")
def days() -> list[day_module.Day]:
    """Дни окна: слепок дня 6 собирается переигровкой дней 0…6."""
    return [
        day_module.stream(CANONICAL_SEED, number)
        for number in range(world.ORDER_WINDOW_DAYS)
    ]


def snapshot_records(days: list[day_module.Day], number: int) -> list[dict]:
    """Слепок дня `number` разобранным обратно из канонических байтов."""
    window = [
        today.orders for today in days[max(0, number - FULL_WINDOW_DAY) : number + 1]
    ]
    return [json.loads(payload) for payload in serialize.orders(window, number)]


def test_the_record_matches_the_wire_contract(days: list[day_module.Day]):
    """Запись слепка — ровно то, что примет строгий приём хранилища.

    Набор и порядок ключей, деньги строками с двумя знаками, времена RFC 3339
    в UTC с миллисекундами, `items` обычным массивом — всё это контракт
    провода, а не вкус: разбор в ODS сверяет набор ключей и форму значений, и
    строка мимо формы уходит в брак целиком (спецификация приёма заказов).

    Деньги сверяются с позициями и между собой: строка на проводе обязана
    отвечать тому же миру, что и заказ в памяти. Цена позиции — цена каталога,
    а `created_at` — время той самой покупки, что уехала в трекер: строка
    заказа создаётся синхронно с ней.
    """
    goods = catalog.catalog()
    price = dict(zip(goods.sku.tolist(), goods.price.tolist(), strict=True))
    bought = _purchase_times(days[SHORT_WINDOW_DAY])
    records = snapshot_records(days, SHORT_WINDOW_DAY)
    assert records

    for record in records:
        assert list(record) == list(CONTRACT)
        assert isinstance(record["user_id"], int)
        assert record["status"] in STATUSES
        assert MOMENT.fullmatch(record["created_at"])
        assert MOMENT.fullmatch(record["updated_at"])
        assert record["snapshot_date"] == _date(SHORT_WINDOW_DAY).isoformat()

        assert isinstance(record["items"], list)
        assert record["items"]
        lines = Decimal(0)
        for item in record["items"]:
            assert list(item) == list(ITEM)
            assert MONEY.fullmatch(item["price"])
            assert isinstance(item["qty"], int)
            assert item["qty"] > 0
            assert Decimal(item["price"]) * 100 == price[item["sku"]]
            lines += Decimal(item["price"]) * item["qty"]

        money = {name: record[name] for name in CONTRACT[5:9]}
        for value in money.values():
            assert MONEY.fullmatch(value)
        assert lines == Decimal(money["items_total"])
        assert Decimal(money["total"]) == (
            Decimal(money["items_total"])
            - Decimal(money["discount"])
            + Decimal(money["delivery"])
        )

        if _born(record) == _date(SHORT_WINDOW_DAY):
            # Потерянного purchase в трекере нет, но заказ остается.
            if record["order_id"] in bought:
                assert record["created_at"][:19] + "Z" == bought[record["order_id"]]


def test_negative_money_does_not_leave_the_source(days: list[day_module.Day]):
    """Сериализатор отвергает деньги, которых контракт провода не допускает."""
    honest = days[0].orders
    total = honest.total.copy()
    total[0] = -1

    with pytest.raises(ValueError):
        serialize.orders([replace(honest, total=total)], honest.day)


def test_the_moments_carry_real_milliseconds(days: list[day_module.Day]):
    """Миллисекунды — часы базы источника, а не три дописанных нуля.

    Три знака дробной части контракт требует всегда, и `.000` формально им
    отвечают — но тогда миллисекундная точность была бы обещанием без модели за
    ним, а разбор в `DateTime64(3)` показывал бы менти ровную секундную сетку
    там, где у источника её нет (исследование формата слепка).

    Фаза у обоих моментов одна: часы базы ставят её строке при создании, а
    судьба заказа отмеряется от неё целыми секундами.
    """
    records = snapshot_records(days, SHORT_WINDOW_DAY)
    assert records

    fractions = {record["created_at"][20:23] for record in records}
    assert fractions - {"000"}

    for record in records:
        assert record["updated_at"][20:23] == record["created_at"][20:23]


def test_the_rows_go_in_the_order_of_birth(days: list[day_module.Day]):
    """Порядок строк слепка — порядок рождения заказов, он же рост номера.

    Хешу слепка в описи нужен именно названный порядок: детерминизм даёт его
    даром, но обещание побайтового повтора держится на нём, а не на удаче.
    """
    numbers = [record["order_id"] for record in snapshot_records(days, FULL_WINDOW_DAY)]
    assert numbers
    assert numbers == sorted(numbers)
    assert len(set(numbers)) == len(numbers)


def test_the_state_is_read_at_the_boundary_of_the_day(days: list[day_module.Day]):
    """Дыхание окна: оплаченный назавтра заказ в сегодняшнем слепке `created`.

    Это и есть состояние на границе суток: учитываются моменты не позже
    границы, поэтому оплата следующего дня в сегодняшний слепок не попадает.
    `updated_at` — поздний учтённый момент, а без единого — `created_at`:
    заказ, с которым на границе ещё ничего не случилось, показывает своё
    рождение.

    Граница модельных суток — полночь пояса счётчика: по нему считается
    модельный день, а `created_at` и `updated_at` уезжают абсолютной меткой.
    """
    before = {
        record["order_id"]: record
        for record in snapshot_records(days, SHORT_WINDOW_DAY)
    }
    after = {
        record["order_id"]: record
        for record in snapshot_records(days, SHORT_WINDOW_DAY + 1)
    }

    breathed = [
        number
        for number, record in before.items()
        if record["status"] == "created" and after[number]["status"] == "paid"
    ]
    assert breathed

    for number in breathed:
        assert before[number]["updated_at"] == before[number]["created_at"]
        assert after[number]["created_at"] == before[number]["created_at"]
        assert after[number]["updated_at"] > after[number]["created_at"]

    edge = _boundary(SHORT_WINDOW_DAY)
    for record in before.values():
        assert record["updated_at"] <= edge


def test_the_run_sends_the_snapshot_of_the_day_before(
    days: list[day_module.Day], tmp_path
):
    """Прогон дня D отправляет слепок дня D−1, и окно усекается у начала оси.

    Сдвиг «ночная выгрузка за вчера» живёт в проигрывателе, поэтому и
    спрашивается с него: прогоны дней 4…7 обязаны отправить слепки дней 3…6 —
    теми же байтами, какие даёт сериализатор, по строке на заказ.
    """
    path = tmp_path / "snapshots.jsonl"
    with closing(FileSink(path)) as sink:
        player.play_snapshots(
            sink,
            seed=CANONICAL_SEED,
            first_day=SHORT_WINDOW_DAY + 1,
            days=FULL_WINDOW_DAY - SHORT_WINDOW_DAY + 1,
        )

    assert path.read_bytes() == b"".join(
        payload + b"\n"
        for number in range(SHORT_WINDOW_DAY, FULL_WINDOW_DAY + 1)
        for payload in serialize.orders(
            [today.orders for today in days[: number + 1]], number
        )
    )

    sent = [json.loads(line) for line in path.read_bytes().splitlines()]
    for number in (SHORT_WINDOW_DAY, FULL_WINDOW_DAY):
        window = {
            _born(record)
            for record in sent
            if record["snapshot_date"] == _date(number).isoformat()
        }
        assert window == {
            _date(born) for born in range(max(0, number - FULL_WINDOW_DAY), number + 1)
        }


def test_the_range_plays_every_day_of_its_windows_once(monkeypatch, tmp_path):
    """Диапазон одним прогоном переигрывает каждый день окна один раз.

    Окна соседних слепков перекрываются почти целиком, и без переиспользования
    стартовый диапазон стоил бы полусотни проигрышей вместо восьми. День здесь
    настоящий: считается не содержимое, а то, сколько раз его спросили.
    """
    asked: list[int] = []
    honest = day_module.stream

    def spy(seed: int, number: int) -> day_module.Day:
        asked.append(number)
        return honest(seed, number)

    monkeypatch.setattr(player.day_module, "stream", spy)
    with closing(FileSink(tmp_path / "range.jsonl")) as sink:
        player.play_snapshots(sink, seed=CANONICAL_SEED, first_day=1, days=3)

    assert sorted(asked) == [0, 1, 2]


def test_the_first_run_sends_nothing(tmp_path):
    """На старте оси дня −1 нет: прогону дня 0 отправлять нечего.

    Не ошибка и не пустой слепок, а отсутствие выгрузки: первый слепок — дня 0
    — уезжает прогоном дня 1.
    """
    path = tmp_path / "day-zero.jsonl"
    assert cli.main(["snapshot", "--day", "0", "--file", str(path)]) == 0
    assert path.read_bytes() == b""


def test_two_runs_give_the_same_snapshot(tmp_path):
    """Пересъёмка слепка даёт те же байты — тем и лечится пропущенный день.

    Прогоны идут разными процессами по тому же доводу, что и у дня событий:
    внутри одного интерпретатора общее зерно хеширования спрятало бы
    зависимость канона от порядка обхода множества.
    """
    first, second = tmp_path / "first.jsonl", tmp_path / "second.jsonl"
    _run_apart(first)
    _run_apart(second)

    assert first.read_bytes()
    assert _digest(first) == _digest(second)


def _run_apart(path) -> None:
    """Снять слепок отдельным процессом; окружение он берёт от нас."""
    finished = subprocess.run(
        [
            sys.executable,
            "-m",
            "clickstream_generator",
            "snapshot",
            "--day",
            "2",
            "--file",
            str(path),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert finished.returncode == 0, finished.stderr


def _purchase_times(today: day_module.Day) -> dict[str, str]:
    """Когда покупка уехала в трекер — по номеру заказа, абсолютной меткой."""
    here = today.columns["EventType"] == commerce.PURCHASE
    numbers = [cell[0] for cell in today.columns["purchaseID"][here]]
    times = np.datetime_as_string(
        today.columns["UTCEventTime"][here], unit="s", timezone="UTC"
    )
    result: dict[str, str] = {}
    for number, moment in zip(numbers, times.tolist(), strict=True):
        result.setdefault(number, moment)
    return result


def _born(record: dict) -> date:
    """День рождения заказа: его номер начинается датой дня покупки."""
    return datetime.strptime(record["order_id"].split("-")[0], "%Y%m%d").date()


def _date(number: int) -> date:
    return world.ORIGIN + timedelta(days=number)


def _boundary(number: int) -> str:
    """Граница суток `number`|`number+1` — полночь пояса счётчика в UTC."""
    midnight = datetime.combine(_date(number + 1), time.min, UTC)
    edge = midnight - timedelta(minutes=world.COUNTER_TIMEZONE_MINUTES)
    return edge.strftime("%Y-%m-%dT%H:%M:%S.") + f"{edge.microsecond // 1000:03d}Z"


def _digest(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
