#!/usr/bin/env bash
set -Eeuo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CLUSTER="clickstream_cluster"
readonly LOCAL_TABLE="smoke_replicated_local"
readonly DISTRIBUTED_TABLE="smoke_distributed"

compose() {
    docker compose --project-directory "$ROOT_DIR" "$@"
}

# Ввод закрыт намеренно: у запроса INSERT clickhouse-client дочитывает данные
# из стандартного ввода и ждёт его конца. Если проверку запустили не из
# терминала, а из фонового процесса с открытым вводом, конца не наступает
# никогда, и команда висит без сообщений.
query() {
    local service="$1"
    local sql="$2"
    compose exec -T "$service" clickhouse-client --query "$sql" </dev/null
}

query_with_timeout() {
    local timeout_seconds="$1"
    local service="$2"
    local sql="$3"
    timeout --foreground "${timeout_seconds}s" \
        docker compose --project-directory "$ROOT_DIR" exec -T "$service" \
        clickhouse-client --query "$sql"
}

fail() {
    printf 'ОШИБКА: %s\n' "$1" >&2
    exit 1
}

assert_equal() {
    local expected="$1"
    local actual="$2"
    local message="$3"
    [[ "$actual" == "$expected" ]] || fail "$message: ожидалось «$expected», получено «$actual»"
}

ensure_stand_running() {
    local running_services
    local service
    running_services="$(compose ps --status running --services 2>/dev/null || true)"
    for service in clickhouse-keeper clickhouse-01 clickhouse-02; do
        if ! grep -qx "$service" <<<"$running_services"; then
            fail "стенд запущен не полностью; выполните make up и дождитесь здорового состояния контейнеров"
        fi
    done
}

cleanup_tables() {
    local cleanup_failed=0
    query_with_timeout 5 clickhouse-01 "DROP TABLE IF EXISTS default.${DISTRIBUTED_TABLE} ON CLUSTER ${CLUSTER} SYNC" >/dev/null 2>&1 || cleanup_failed=1
    query_with_timeout 5 clickhouse-01 "DROP TABLE IF EXISTS default.${LOCAL_TABLE} ON CLUSTER ${CLUSTER} SYNC" >/dev/null 2>&1 || cleanup_failed=1
    return "$cleanup_failed"
}

on_exit() {
    local status=$?
    trap - EXIT
    if [[ "$status" -ne 0 ]]; then
        printf 'Сбой проверки: выполняется быстрая очистка временных таблиц...\n' >&2
    fi
    cleanup_tables || printf 'ПРЕДУПРЕЖДЕНИЕ: очистка не завершена; после восстановления стенда повторите make smoke-cluster.\n' >&2
    exit "$status"
}

on_signal() {
    trap - EXIT INT TERM
    printf 'Проверка прервана пользователем: выполняется быстрая очистка временных таблиц...\n' >&2
    cleanup_tables || printf 'ПРЕДУПРЕЖДЕНИЕ: очистка не завершена; после восстановления стенда повторите make smoke-cluster.\n' >&2
    exit 130
}

assert_ddl_queue_completed() {
    local service="$1"
    local phase="$2"
    local queue_state
    local total
    local unfinished
    queue_state="$(query "$service" "SELECT count(), countIf(status NOT IN ('Finished', 'Removing')) FROM system.distributed_ddl_queue FORMAT TSV")"
    total="${queue_state%%$'\t'*}"
    unfinished="${queue_state##*$'\t'}"
    [[ "$total" -gt 0 ]] || fail "на ${service} очередь DDL пуста ${phase}"
    assert_equal "0" "$unfinished" "на ${service} есть незавершённые DDL ${phase}"
}

ensure_stand_running
trap on_exit EXIT
trap on_signal INT TERM

printf 'Проверка 1/8: описание кластера одинаково на обеих нодах...\n'
cluster_sql="SELECT cluster, shard_num, replica_num, host_name, port FROM system.clusters WHERE cluster = '${CLUSTER}' ORDER BY shard_num, replica_num FORMAT TSV"
cluster_01="$(query clickhouse-01 "$cluster_sql")"
cluster_02="$(query clickhouse-02 "$cluster_sql")"
expected_cluster=$'clickstream_cluster\t1\t1\tclickhouse-01\t9000\nclickstream_cluster\t2\t1\tclickhouse-02\t9000'
assert_equal "$expected_cluster" "$cluster_01" "неверная топология на первой ноде"
assert_equal "$expected_cluster" "$cluster_02" "неверная топология на второй ноде"
printf 'ЗЕЛЁНО: обе ноды видят ожидаемые два шарда: clickhouse-01 и clickhouse-02.\n'

printf 'Проверка 2/8: у нод разные макросы shard и replica...\n'
macros_sql="SELECT macro, substitution FROM system.macros WHERE macro IN ('shard', 'replica') ORDER BY macro FORMAT TSV"
macros_01="$(query clickhouse-01 "$macros_sql")"
macros_02="$(query clickhouse-02 "$macros_sql")"
assert_equal $'replica\tclickhouse-01\nshard\t01' "$macros_01" "неверные макросы первой ноды"
assert_equal $'replica\tclickhouse-02\nshard\t02' "$macros_02" "неверные макросы второй ноды"
[[ "$macros_01" != "$macros_02" ]] || fail "макросы нод не должны совпадать"
printf 'ЗЕЛЁНО: clickhouse-01=(shard 01, replica clickhouse-01), clickhouse-02=(shard 02, replica clickhouse-02).\n'

printf 'Проверка 3/8: keeper отвечает обеим нодам...\n'
query clickhouse-01 "SELECT name FROM system.zookeeper WHERE path = '/' ORDER BY name FORMAT Null"
query clickhouse-02 "SELECT name FROM system.zookeeper WHERE path = '/' ORDER BY name FORMAT Null"
printf 'ЗЕЛЁНО: system.zookeeper доступна с обеих нод.\n'

cleanup_tables || fail "не удалось очистить объекты предыдущего запуска"

printf 'Проверка 4/8: ReplicatedMergeTree создаётся через ON CLUSTER...\n'
query clickhouse-01 "
    CREATE TABLE default.${LOCAL_TABLE} ON CLUSTER ${CLUSTER}
    (
        ClientID UInt64,
        value String
    )
    ENGINE = ReplicatedMergeTree(
        '/clickhouse/tables/{shard}/${LOCAL_TABLE}',
        '{replica}'
    )
    ORDER BY ClientID
" >/dev/null
tables_sql="SELECT name, engine FROM system.tables WHERE database = 'default' AND name = '${LOCAL_TABLE}' FORMAT TSV"
assert_equal $'smoke_replicated_local\tReplicatedMergeTree' "$(query clickhouse-01 "$tables_sql")" "локальная таблица не создана на первой ноде"
assert_equal $'smoke_replicated_local\tReplicatedMergeTree' "$(query clickhouse-02 "$tables_sql")" "локальная таблица не создана на второй ноде"
printf 'ЗЕЛЁНО: ReplicatedMergeTree видна в system.tables на обеих нодах.\n'

printf 'Проверка 5/8: путь в keeper собран из макроса shard...\n'
path_sql="SELECT zookeeper_path, replica_name FROM system.replicas WHERE database = 'default' AND table = '${LOCAL_TABLE}' FORMAT TSV"
assert_equal "/clickhouse/tables/01/${LOCAL_TABLE}"$'\t'"clickhouse-01" "$(query clickhouse-01 "$path_sql")" "неверные путь или имя реплики на первой ноде"
assert_equal "/clickhouse/tables/02/${LOCAL_TABLE}"$'\t'"clickhouse-02" "$(query clickhouse-02 "$path_sql")" "неверные путь или имя реплики на второй ноде"
printf 'ЗЕЛЁНО: пути собраны из shard (/01/ и /02/), имя реплики собрано из макроса replica.\n'

printf 'Проверка 6/8: Distributed создаётся ON CLUSTER и передаёт данные между нодами...\n'
query clickhouse-01 "
    CREATE TABLE default.${DISTRIBUTED_TABLE} ON CLUSTER ${CLUSTER}
    AS default.${LOCAL_TABLE}
    ENGINE = Distributed(
        '${CLUSTER}',
        'default',
        '${LOCAL_TABLE}',
        cityHash64(ClientID)
    )
" >/dev/null
distributed_sql="SELECT engine FROM system.tables WHERE database = 'default' AND name = '${DISTRIBUTED_TABLE}' FORMAT TSVRaw"
assert_equal "Distributed" "$(query clickhouse-01 "$distributed_sql")" "Distributed-таблица не создана на первой ноде"
assert_equal "Distributed" "$(query clickhouse-02 "$distributed_sql")" "Distributed-таблица не создана на второй ноде"
query clickhouse-01 "INSERT INTO default.${LOCAL_TABLE} VALUES (42, 'из первой ноды')"
read_back="$(query clickhouse-02 "SELECT ClientID, value FROM default.${DISTRIBUTED_TABLE} WHERE ClientID = 42 FORMAT TSV")"
assert_equal $'42\tиз первой ноды' "$read_back" "вторая нода не прочитала вставленную строку"
query clickhouse-01 "INSERT INTO default.${DISTRIBUTED_TABLE} SETTINGS distributed_foreground_insert = 1 SELECT number + 1000, 'через Distributed' FROM numbers(16)"
sharding_sql="SELECT countIf(_shard_num != cityHash64(ClientID) % 2 + 1), uniqExact(_shard_num) FROM default.${DISTRIBUTED_TABLE} WHERE value = 'через Distributed' FORMAT TSV"
assert_equal $'0\t2' "$(query clickhouse-02 "$sharding_sql")" "Distributed использует неверный ключ шардирования"
printf 'ЗЕЛЁНО: локальная строка первой ноды читается со второй; ключ cityHash64(ClientID) разложил строки по двум шардам.\n'

printf 'Проверка 7/8: в очереди распределённых DDL нет незавершённых заданий...\n'
assert_ddl_queue_completed clickhouse-01 'после CREATE'
assert_ddl_queue_completed clickhouse-02 'после CREATE'
printf 'ЗЕЛЁНО: очередь содержит задания CREATE, незавершённых среди них нет.\n'

printf 'Проверка 8/8: временные таблицы удаляются через ON CLUSTER...\n'
query clickhouse-01 "DROP TABLE default.${DISTRIBUTED_TABLE} ON CLUSTER ${CLUSTER} SYNC" >/dev/null
query clickhouse-01 "DROP TABLE default.${LOCAL_TABLE} ON CLUSTER ${CLUSTER} SYNC" >/dev/null
remaining_sql="SELECT count() FROM system.tables WHERE database = 'default' AND name IN ('${LOCAL_TABLE}', '${DISTRIBUTED_TABLE}') FORMAT TSVRaw"
assert_equal "0" "$(query clickhouse-01 "$remaining_sql")" "временные таблицы остались на первой ноде"
assert_equal "0" "$(query clickhouse-02 "$remaining_sql")" "временные таблицы остались на второй ноде"
assert_ddl_queue_completed clickhouse-01 'после DROP'
assert_ddl_queue_completed clickhouse-02 'после DROP'
trap - EXIT INT TERM
printf 'ЗЕЛЁНО: временные таблицы удалены; проверены завершённые задания CREATE и DROP.\n'
printf 'ИТОГ: все 8 проверок кластера ClickHouse прошли.\n'
