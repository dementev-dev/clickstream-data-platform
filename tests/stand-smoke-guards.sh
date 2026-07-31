#!/usr/bin/env bash
set -uo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN:-docker compose}"
readonly LOG_FILE="$(mktemp)"
restored=0
passed=0

compose() {
    "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" "$@"
}

restore_stand() {
    make --no-print-directory -C "$ROOT_DIR" COMPOSE="${COMPOSE_BIN:-docker compose}" up >/dev/null
}

on_exit() {
    local status=$?
    trap - EXIT INT TERM
    rm -f "$LOG_FILE"
    if [[ "$restored" -eq 0 ]] && ! restore_stand; then
        printf 'ОШИБКА: не удалось восстановить стенд после проверки.\n' >&2
        status=1
    fi
    exit "$status"
}

trap on_exit EXIT
trap 'exit 130' INT TERM

if make --no-print-directory -C "$ROOT_DIR" COMPOSE="${COMPOSE_BIN:-docker compose}" smoke >"$LOG_FILE" 2>&1; then
    passed=$((passed + 1))
    printf 'ЗЕЛЁНО: перед проверкой отказа make smoke проходит полностью.\n'
else
    printf 'ОШИБКА: исходный стенд не проходит make smoke; проверка отказа недостоверна.\n' >&2
    exit 1
fi

compose stop prometheus >/dev/null
make --no-print-directory -C "$ROOT_DIR" COMPOSE="${COMPOSE_BIN:-docker compose}" smoke >"$LOG_FILE" 2>&1
status=$?

if [[ "$status" -ne 0 ]] && grep -q 'ОШИБКА: сервис prometheus' "$LOG_FILE"; then
    passed=$((passed + 1))
    printf 'ЗЕЛЁНО: остановленный prometheus делает make smoke красным и назван в отчёте.\n'
else
    printf 'ОШИБКА: make smoke не обнаружил остановленный prometheus; код=%s.\n' "$status" >&2
    exit 1
fi

if ! restore_stand; then
    printf 'ОШИБКА: не удалось восстановить стенд после проверки.\n' >&2
    exit 1
fi
restored=1

if make --no-print-directory -C "$ROOT_DIR" COMPOSE="${COMPOSE_BIN:-docker compose}" smoke >"$LOG_FILE" 2>&1; then
    passed=$((passed + 1))
    printf 'ЗЕЛЁНО: после восстановления make smoke снова проходит полностью.\n'
else
    printf 'ОШИБКА: после восстановления стенд не проходит make smoke.\n' >&2
    exit 1
fi

rm -f "$LOG_FILE"
trap - EXIT INT TERM
printf 'ИТОГ: пройдено %d, ошибок 0\n' "$passed"
