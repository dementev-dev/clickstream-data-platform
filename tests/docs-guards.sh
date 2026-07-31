#!/usr/bin/env bash
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly README="$ROOT_DIR/README.md"

grep -Eq 'нода 1.*28123.*29000' "$README"
grep -Eq 'нода 2.*28124.*29001' "$README"
printf 'ЗЕЛЁНО: README перечисляет HTTP- и нативные порты обеих нод.\n'

grep -Eq 'После первого запуска.*`make clean`' "$README"
printf 'ЗЕЛЁНО: README объясняет сброс томов после смены исходных учётных данных.\n'

grep -Fxq -- '- нода 2 — `http://127.0.0.1:28124`, нативный порт `29001`;' "$README"
printf 'ЗЕЛЁНО: список портов остаётся единым списком.\n'

awk '
    previous == "После изменения `infra/clickhouse/config.d/prometheus.xml` выполните" &&
        $0 == "`docker compose restart clickhouse-01 clickhouse-02`: обычный `make up` не" {
        found = 1
    }
    {previous = $0}
    END {exit !found}
' "$README"
printf 'ЗЕЛЁНО: README требует перезапуск ClickHouse после изменения настройки метрик.\n'

grep_status=0
grep -q '3,4 GB' "$ROOT_DIR/scripts/stand-smoke.sh" || grep_status=$?
if [[ "$grep_status" -eq 0 ]]; then
    printf 'ОШИБКА: отчёт проверки использует латинское обозначение GB.\n' >&2
    exit 1
elif [[ "$grep_status" -ne 1 ]]; then
    printf 'ОШИБКА: не удалось проверить обозначение единицы памяти.\n' >&2
    exit 1
fi
printf 'ЗЕЛЁНО: отчёт проверки использует русское обозначение ГБ.\n'

printf 'ИТОГ: пройдено 5, ошибок 0\n'
