#!/usr/bin/env bash
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly FIXTURE_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "$FIXTURE_DIR"
}

trap cleanup EXIT
mkdir -p "$FIXTURE_DIR/bin" "$FIXTURE_DIR/scripts"
cp "$ROOT_DIR/scripts/stand-smoke.sh" "$FIXTURE_DIR/scripts/stand-smoke.sh"
cp "$(type -P false)" "$FIXTURE_DIR/bin/docker"

set +e
output="$(PATH="$FIXTURE_DIR/bin:$PATH" COMPOSE_BIN=false \
    "$FIXTURE_DIR/scripts/stand-smoke.sh" 2>&1)"
status=$?
set -e

error_message_status=0
grep -q 'ОШИБКА: compose.yaml или .env.example недоступны для чтения' \
    <<<"$output" || error_message_status=$?
green_message_status=0
grep -q 'ЗЕЛЁНО: .env.example совпадает' <<<"$output" || green_message_status=$?
if [[ "$status" -ne 0 ]] && \
    [[ "$error_message_status" -eq 0 ]] && \
    [[ "$green_message_status" -eq 1 ]]; then
    printf 'ЗЕЛЁНО: недоступные compose.yaml и .env.example не проходят статическую проверку.\n'
else
    printf 'ОШИБКА: статическая проверка приняла недоступные файлы.\n' >&2
    exit 1
fi

cp "$ROOT_DIR/compose.yaml" "$ROOT_DIR/.env.example" "$FIXTURE_DIR/"
set +e
timeout 3s env LC_ALL=ru_RU.UTF-8 PATH="$FIXTURE_DIR/bin:$PATH" COMPOSE_BIN=false \
    "$FIXTURE_DIR/scripts/stand-smoke.sh" >/dev/null 2>&1
status=$?
set -e
if [[ "$status" -ne 124 ]]; then
    printf 'ЗЕЛЁНО: статическая проверка укладывается в три секунды при русской локали.\n'
else
    printf 'ОШИБКА: статическая проверка превысила три секунды при русской локали.\n' >&2
    exit 1
fi

printf 'ИТОГ: пройдено 2, ошибок 0\n'
