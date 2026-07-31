#!/usr/bin/env bash
set -uo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly LOCAL_TABLE="smoke_replicated_local"
readonly DISTRIBUTED_TABLE="smoke_distributed"
passed=0
failed=0

compose() {
    docker compose --project-directory "$ROOT_DIR" "$@"
}

query() {
    local service="$1"
    local sql="$2"
    compose exec -T "$service" clickhouse-client --query "$sql"
}

pass() {
    passed=$((passed + 1))
    printf 'ЗЕЛЁНО: %s.\n' "$1"
}

fail() {
    failed=$((failed + 1))
    printf 'ОШИБКА: %s.\n' "$1" >&2
}

wait_for_local_table() {
    local attempt
    for attempt in $(seq 1 100); do
        if [[ "$(query clickhouse-01 "SELECT count() FROM system.tables WHERE database = 'default' AND name = '${LOCAL_TABLE}' FORMAT TSVRaw" 2>/dev/null || true)" == '1' ]]; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

restore_stand() {
    make --no-print-directory -C "$ROOT_DIR" up >/dev/null
    query clickhouse-01 "DROP TABLE IF EXISTS default.${DISTRIBUTED_TABLE} ON CLUSTER clickstream_cluster SYNC" >/dev/null 2>&1 || true
    query clickhouse-01 "DROP TABLE IF EXISTS default.${LOCAL_TABLE} ON CLUSTER clickstream_cluster SYNC" >/dev/null 2>&1 || true
}

check_failure_guards() {
    local failure_log
    local failure_pid
    local failure_status
    local failure_elapsed
    local interrupt_log
    local interrupt_pid
    local interrupt_status
    local remaining_tables
    local started_at
    local bounded_failure_ok=0
    local interrupt_ok=0
    interrupt_status=999
    remaining_tables='не проверено'

    failure_log="$(mktemp)"
    started_at="$(date +%s)"
    setsid make --no-print-directory -C "$ROOT_DIR" smoke-cluster >"$failure_log" 2>&1 &
    failure_pid=$!
    if wait_for_local_table; then
        compose kill clickhouse-02 >/dev/null
        wait "$failure_pid"
        failure_status=$?
        failure_elapsed=$(( $(date +%s) - started_at ))
        if [[ "$failure_status" -eq 2 ]] && [[ "$failure_elapsed" -lt 20 ]]; then
            bounded_failure_ok=1
        fi
    else
        kill -TERM -- "-$failure_pid" >/dev/null 2>&1 || true
        wait "$failure_pid" >/dev/null 2>&1 || true
    fi
    rm -f "$failure_log"

    restore_stand

    interrupt_log="$(mktemp)"
    setsid env --default-signal=INT,TERM "$ROOT_DIR/scripts/clickhouse-smoke.sh" >"$interrupt_log" 2>&1 &
    interrupt_pid=$!
    if wait_for_local_table; then
        kill -INT -- "-$interrupt_pid"
        wait "$interrupt_pid"
        interrupt_status=$?
        remaining_tables="$(query clickhouse-01 "SELECT count() FROM clusterAllReplicas('clickstream_cluster', system.tables) WHERE database = 'default' AND name IN ('${LOCAL_TABLE}', '${DISTRIBUTED_TABLE}') FORMAT TSVRaw")"
        if [[ "$interrupt_status" -eq 130 ]] && [[ "$remaining_tables" == '0' ]]; then
            interrupt_ok=1
        fi
    else
        kill -TERM -- "-$interrupt_pid" >/dev/null 2>&1 || true
        wait "$interrupt_pid" >/dev/null 2>&1 || true
    fi
    rm -f "$interrupt_log"

    if [[ "$bounded_failure_ok" -eq 1 ]] && [[ "$interrupt_ok" -eq 1 ]]; then
        pass 'аварийная очистка ограничена по времени, SIGINT возвращает 130 и удаляет временные таблицы'
    else
        fail "нарушена аварийная семантика smoke: bounded=${bounded_failure_ok}, interrupt=${interrupt_ok}, status=${interrupt_status}, tables=${remaining_tables}"
    fi
}

check_keeper_runtime() {
    local keeper_user
    local keeper_nofile
    local keeper_owner
    keeper_user="$(compose exec -T clickhouse-keeper id -un)"
    keeper_nofile="$(compose exec -T clickhouse-keeper awk '$1 == "Max" && $2 == "open" && $3 == "files" {print $4}' /proc/1/limits)"
    keeper_owner="$(compose exec -T clickhouse-keeper stat -c '%U:%G' /var/lib/clickhouse/coordination)"
    if [[ "$keeper_user" == 'clickhouse' ]] && [[ "$keeper_nofile" -ge 262144 ]] && [[ "$keeper_owner" == 'clickhouse:clickhouse' ]]; then
        pass 'keeper работает от clickhouse с nofile 262144 и своим каталогом данных'
    else
        fail "неверное окружение keeper: user=${keeper_user}, nofile=${keeper_nofile}, owner=${keeper_owner}"
    fi
}

check_preflight_hint() {
    local output
    local status
    compose kill clickhouse-02 >/dev/null
    output="$("$ROOT_DIR/scripts/clickhouse-smoke.sh" 2>&1)"
    status=$?
    if [[ "$status" -ne 0 ]] && grep -q 'выполните make up' <<<"$output"; then
        pass 'неполный стенд получает русскую подсказку выполнить make up'
    else
        fail 'нет русской подсказки для незапущенного стенда'
    fi
    make --no-print-directory -C "$ROOT_DIR" up >/dev/null
}

check_failure_guards
check_keeper_runtime
check_preflight_hint

printf 'ИТОГ: пройдено %d, ошибок %d\n' "$passed" "$failed"
[[ "$failed" -eq 0 ]]
