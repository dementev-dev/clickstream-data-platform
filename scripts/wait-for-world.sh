#!/usr/bin/env bash
# Ждёт, пока стартовый мир доедет до ODS. Второй шаг `make up`, и вот зачем он.
#
# `docker compose up --wait` дожидается служб, в том числе успешно отработавшей
# заливки. Но «заливка кончилась» — это «события лежат в топике», а не «события
# в хранилище»: приём асинхронный. Движок Kafka копит блок и отдаёт его по
# размеру либо по `stream_flush_interval_ms` (умолчание 7,5 с), а стартовый мир —
# около 400 тыс. сообщений. Верни `make up` управление сразу — и `make
# check-clickhouse` следом покраснел бы по устройству, а не по поломке.
#
# Ждём ограниченным циклом с потолком, а не паузой наугад: пауза либо коротка на
# медленной машине, либо ворует минуту на быстрой.
#
# Условие ожидания — «всё доехало», а не «всё разобралось»: сумма событий и
# брака. Сломайся разбор — события уедут в `*_errors`, сумма сойдётся, ожидание
# кончится, и поломку назовёт `make check-clickhouse`. Ждать здесь одних годных
# событий значило бы висеть пять минут вместо внятного ответа.
set -Eeuo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly INVENTORY="${ROOT_DIR}/data/world-inventory.json"
readonly ATTEMPTS=100
readonly PAUSE_SECONDS=3
# Как часто отчитываться о ходе: молчащая минуту команда выглядит зависшей.
readonly REPORT_EVERY=5

read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN:-docker compose}"

fail() {
    printf 'ОШИБКА: %s\n' "$1" >&2
    exit 1
}

query() {
    "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" exec -T clickhouse-01 \
        clickhouse-client --query "$1" </dev/null
}

expected="$(jq '[.days[].events] | add' "$INVENTORY")"
first_date="$(jq -r '.days | first | .date' "$INVENTORY")"
last_date="$(jq -r '.days | last | .date' "$INVENTORY")"

printf 'Ждём стартовый мир в ods.event: %s событий за %s — %s...\n' \
    "$expected" "$first_date" "$last_date"

# Счёт без FINAL: здесь спрашивают «доехало ли», а не «сколько их на самом
# деле». Повторная заливка даст больше ожидаемого — и это тоже «доехало».
arrived_sql="
    SELECT
        (SELECT count() FROM ods.event_dist
            WHERE EventDate BETWEEN '${first_date}' AND '${last_date}')
      + (SELECT count() FROM ods.event_errors_dist)
"

for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    arrived="$(query "$arrived_sql" 2>/dev/null || true)"
    if [[ "$arrived" =~ ^[0-9]+$ ]] && ((arrived >= expected)); then
        if ((arrived > expected)); then
            # Повторный `make up` заливает мир заново. Промолчи мы об этом —
            # удвоенное число под «ЗЕЛЁНО» выглядело бы поломкой.
            printf 'ЗЕЛЁНО: стартовый мир доехал; строк %s при ожидаемых %s —' \
                "$arrived" "$expected"
            printf ' мир заливали повторно, повтор схлопнет ReplacingMergeTree.\n'
        else
            printf 'ЗЕЛЁНО: стартовый мир доехал: %s строк.\n' "$arrived"
        fi
        exit 0
    fi
    if ((attempt % REPORT_EVERY == 0)); then
        printf 'Доехало %s из %s, ждём дальше (%d с)...\n' \
            "${arrived:-0}" "$expected" "$((attempt * PAUSE_SECONDS))"
    fi
    sleep "$PAUSE_SECONDS"
done

fail "стартовый мир не доехал за $((ATTEMPTS * PAUSE_SECONDS)) с:
  доехало ${arrived:-0} из ${expected}. Смотрите журнал заливки
  (docker compose logs world-init) и чтеца топика:
  SELECT * FROM system.kafka_consumers"
