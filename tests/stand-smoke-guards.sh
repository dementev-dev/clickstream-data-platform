#!/usr/bin/env bash
set -uo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN:-docker compose}"
readonly LOG_FILE="$(mktemp)"
readonly CLICKHOUSE_LOCAL_TABLE="airflow_probe_local"
readonly CLICKHOUSE_DISTRIBUTED_TABLE="airflow_probe_distributed"
restored=0
passed=0
red_smoke_pid=''

compose() {
    "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" "$@"
}

published_port() {
    local binding
    local service="$1"
    local container_port="$2"
    binding="$(compose port "$service" "$container_port" 2>/dev/null || true)"
    printf '%s\n' "${binding##*:}"
}

query_node_2() {
    local sql="$1"
    local port
    port="$(published_port clickhouse-02 8123)"
    curl -sf --max-time 2 --data-binary "$sql" "http://127.0.0.1:${port}/"
}

break_clickhouse_probe() {
    local deadline
    local table_state

    deadline=$((EPOCHSECONDS + 60))
    printf 'Ожидание служебных таблиц на ноде 2, предел 60 секунд...\n'
    while [[ "$EPOCHSECONDS" -lt "$deadline" ]]; do
        table_state="$(query_node_2 "
            SELECT
                countIf(name = '${CLICKHOUSE_LOCAL_TABLE}'),
                countIf(name = '${CLICKHOUSE_DISTRIBUTED_TABLE}')
            FROM system.tables
            WHERE database = 'default'
              AND name IN (
                  '${CLICKHOUSE_LOCAL_TABLE}',
                  '${CLICKHOUSE_DISTRIBUTED_TABLE}'
              )
            FORMAT TSV
        " 2>/dev/null || true)"
        if [[ "$table_state" == $'1\t1' ]]; then
            query_node_2 \
                "DROP TABLE IF EXISTS default.${CLICKHOUSE_LOCAL_TABLE} SYNC" \
                >/dev/null
            table_state="$(query_node_2 "
                SELECT
                    countIf(name = '${CLICKHOUSE_LOCAL_TABLE}'),
                    countIf(name = '${CLICKHOUSE_DISTRIBUTED_TABLE}')
                FROM system.tables
                WHERE database = 'default'
                  AND name IN (
                      '${CLICKHOUSE_LOCAL_TABLE}',
                      '${CLICKHOUSE_DISTRIBUTED_TABLE}'
                  )
                FORMAT TSV
            " 2>/dev/null || true)"
            if [[ "$table_state" == $'0\t1' ]]; then
                printf 'Служебная локальная таблица удалена только на ноде 2.\n'
                return
            fi
            return 1
        fi
        sleep 0.05
    done
    return 1
}

# Пробник разбит на четыре задачи, и упасть может любая из них: поломка на ноде
# 2 видна и проверке набора таблиц, и чтению маркера. Поэтому берём последний
# каталог запуска целиком и ищем образец по журналам всех его задач.
clickhouse_break_is_reported() {
    compose exec -T airflow-scheduler bash -ceu '
        latest="$(
            find /opt/airflow/logs/dag_id=test_clickhouse \
                -mindepth 1 -maxdepth 1 -type d -name "run_id=*" -printf "%T@ %p\n" |
                sort -nr |
                head -n 1
        )"
        latest="${latest#* }"
        test -n "$latest"
        grep -Eqr --include="attempt=1.log" \
            "неверный набор таблиц на ноде 2|Unknown table expression identifier '\''default.airflow_probe_local'\''" \
            "$latest"
    '
}

probe_failure_is_reported() {
    local dag_id="$1"
    grep -Eq \
        "ОШИБКА: (пробник ${dag_id}|Airflow не показал пробник ${dag_id})" \
        "$LOG_FILE"
}

restore_stand() {
    make --no-print-directory -C "$ROOT_DIR" COMPOSE="${COMPOSE_BIN:-docker compose}" up >/dev/null
}

on_exit() {
    local status=$?
    trap - EXIT INT TERM
    if [[ -n "$red_smoke_pid" ]]; then
        kill -TERM -- "-$red_smoke_pid" >/dev/null 2>&1 || true
        wait "$red_smoke_pid" >/dev/null 2>&1 || true
    fi
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
setsid make --no-print-directory -C "$ROOT_DIR" \
    COMPOSE="${COMPOSE_BIN:-docker compose}" smoke >"$LOG_FILE" 2>&1 &
red_smoke_pid=$!
if ! break_clickhouse_probe; then
    printf 'ОШИБКА: не удалось удалить служебную таблицу только на ноде 2.\n' >&2
    exit 1
fi
compose stop kafka >/dev/null
wait "$red_smoke_pid"
status=$?
red_smoke_pid=''

red_path_ok=1
if [[ "$status" -eq 0 ]]; then
    printf 'ОШИБКА: make smoke остался зелёным после внесённых поломок.\n' >&2
    red_path_ok=0
fi
if ! grep -q 'ОШИБКА: сервис prometheus' "$LOG_FILE"; then
    printf 'ОШИБКА: make smoke не назвал остановленный prometheus.\n' >&2
    red_path_ok=0
fi
if ! probe_failure_is_reported test_clickhouse; then
    printf 'ОШИБКА: make smoke не назвал пробник test_clickhouse.\n' >&2
    red_path_ok=0
fi
if ! probe_failure_is_reported test_kafka; then
    printf 'ОШИБКА: make smoke не назвал пробник test_kafka.\n' >&2
    red_path_ok=0
fi
if ! clickhouse_break_is_reported; then
    printf 'ОШИБКА: журнал test_clickhouse не связал отказ с удалённой таблицей на ноде 2.\n' >&2
    red_path_ok=0
fi

if [[ "$red_path_ok" -eq 1 ]]; then
    passed=$((passed + 1))
    printf 'ЗЕЛЁНО: удаление таблицы на ноде 2 и остановка prometheus с kafka делают make smoke красным; оба пробника названы в отчёте.\n'
else
    printf 'ОШИБКА: проверка красного пути завершилась с кодом make smoke %s.\n' "$status" >&2
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
