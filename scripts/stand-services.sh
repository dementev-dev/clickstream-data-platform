#!/usr/bin/env bash
# make check-services — службы работают: DAG запускается и доходит, топик
# создаётся и удаляется, Superset логинится и ходит в базу. Здесь собрано всё,
# что ждёт службу, поэтому цель идёт десятки секунд. Карта целей —
# docs/architecture/testing.md.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/stand-common.sh" || exit 1

kafka_image=''
kafka_port=''
kafka_cleanup_topic=''
airflow_port=''
airflow_token=''
airflow_cleanup_dag_id=''
airflow_cleanup_original_paused=''
airflow_cleanup_run_id=''

kafka_cli() {
    timeout 30s docker run --rm --network host "$kafka_image" \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server "localhost:${kafka_port}" "$@"
}

kafka_topic_exists() {
    local list
    local listed_topic

    list="$(kafka_cli --list 2>/dev/null)" || return 2
    while IFS= read -r listed_topic; do
        [[ "$listed_topic" == "$kafka_cleanup_topic" ]] && return 0
    done <<<"$list"
    return 1
}

cleanup_kafka_topic() {
    local attempt
    local topic_status

    [[ -n "$kafka_cleanup_topic" ]] || return 0
    kafka_cli --delete --if-exists --topic "$kafka_cleanup_topic" >/dev/null 2>&1 || true
    for ((attempt = 1; attempt <= 10; attempt++)); do
        kafka_topic_exists
        topic_status=$?
        if [[ "$topic_status" -eq 1 ]]; then
            kafka_cleanup_topic=''
            return 0
        fi
        sleep 1
    done
    return 1
}

cleanup_airflow_run() {
    local encoded_run_id

    [[ -n "$airflow_cleanup_run_id" ]] || return 0
    encoded_run_id="$(jq -rn --arg value "$airflow_cleanup_run_id" '$value | @uri')"
    if curl -sf --max-time 10 -X DELETE \
        -H "Authorization: Bearer ${airflow_token}" \
        "http://127.0.0.1:${airflow_port}/api/v2/dags/${airflow_cleanup_dag_id}/dagRuns/${encoded_run_id}" \
        >/dev/null 2>&1; then
        airflow_cleanup_run_id=''
        return 0
    fi
    return 1
}

cleanup_airflow_pause() {
    local payload

    [[ -n "$airflow_cleanup_original_paused" ]] || return 0
    payload="$(jq -cn --argjson is_paused "$airflow_cleanup_original_paused" \
        '{is_paused: $is_paused}')"
    if curl -sf --max-time 10 -X PATCH \
        -H "Authorization: Bearer ${airflow_token}" \
        -H 'Content-Type: application/json' \
        -d "$payload" \
        "http://127.0.0.1:${airflow_port}/api/v2/dags/${airflow_cleanup_dag_id}" \
        >/dev/null 2>&1; then
        airflow_cleanup_original_paused=''
        return 0
    fi
    return 1
}

on_exit() {
    local status=$?
    trap - EXIT
    cleanup_kafka_topic || true
    cleanup_airflow_run || true
    cleanup_airflow_pause || true
    exit "$status"
}

on_signal() {
    trap - EXIT INT TERM
    cleanup_kafka_topic || true
    cleanup_airflow_run || true
    cleanup_airflow_pause || true
    exit 130
}

trap on_exit EXIT
trap on_signal INT TERM

check_kafka_from_host() {
    local attempt
    local deleted=0
    local topic_status

    kafka_image="$(compose config --format json | jq -r '.services.kafka.image')"
    kafka_port="$(published_port kafka 29092)"
    kafka_cleanup_topic="check_services_${EPOCHSECONDS}_$$_${RANDOM}"

    if [[ -n "$kafka_port" ]] && \
        kafka_cli --create --topic "$kafka_cleanup_topic" --partitions 1 --replication-factor 1 >/dev/null 2>&1 && \
        kafka_topic_exists && \
        kafka_cli --delete --topic "$kafka_cleanup_topic" >/dev/null 2>&1; then
        for ((attempt = 1; attempt <= 10; attempt++)); do
            kafka_topic_exists
            topic_status=$?
            if [[ "$topic_status" -eq 1 ]]; then
                deleted=1
                kafka_cleanup_topic=''
                break
            fi
            sleep 1
        done
    fi

    if [[ "$deleted" -eq 1 ]]; then
        pass "Kafka доступна с машины через localhost:${kafka_port}: временный топик создан, найден и удалён"
        return
    fi
    cleanup_kafka_topic || true
    fail "Kafka недоступна с машины через отображённый порт ${kafka_port:-не найден} или временный топик не исчез после удаления"
}

# Вход в Airflow здесь не проверка, а подготовка: то же самое утверждает make
# smoke, и там оно засчитано. Отдельного ЗЕЛЁНО на этот вход не заводим.
airflow_sign_in() {
    local config
    local password
    local response
    local user

    config="$(compose config --format json 2>/dev/null || true)"
    user="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_USER // empty' <<<"$config")"
    password="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_PASSWORD // empty' <<<"$config")"
    airflow_port="$(published_port airflow-apiserver 8080)"

    response="$(curl -sf --max-time 10 -X POST \
        -H 'Content-Type: application/json' \
        -d "$(jq -cn --arg username "$user" --arg password "$password" \
            '{username: $username, password: $password}')" \
        "http://127.0.0.1:${airflow_port}/auth/token" 2>/dev/null || true)"
    airflow_token="$(jq -r '.access_token // empty' <<<"$response")"
    [[ -n "$airflow_token" ]]
}

run_airflow_probe() {
    local dag
    local dag_id="$1"
    local encoded_run_id
    local response
    local run_state=''
    local unpaused
    local -i attempt

    dag="$(curl -sf --max-time 10 \
        -H "Authorization: Bearer ${airflow_token}" \
        "http://127.0.0.1:${airflow_port}/api/v2/dags/${dag_id}" \
        2>/dev/null || true)"
    if ! jq -e --arg dag_id "$dag_id" '.dag_id == $dag_id' \
        >/dev/null 2>&1 <<<"$dag"; then
        fail "пробник ${dag_id} не виден через API Airflow"
        return 1
    fi

    airflow_cleanup_dag_id="$dag_id"
    airflow_cleanup_original_paused="$(jq -r '.is_paused | tostring' <<<"$dag")"
    unpaused="$(curl -sf --max-time 10 -X PATCH \
        -H "Authorization: Bearer ${airflow_token}" \
        -H 'Content-Type: application/json' \
        -d '{"is_paused":false}' \
        "http://127.0.0.1:${airflow_port}/api/v2/dags/${dag_id}" \
        2>/dev/null || true)"
    if ! jq -e '.is_paused == false' >/dev/null 2>&1 <<<"$unpaused"; then
        fail "Airflow не смог включить пробник ${dag_id} перед ручным запуском"
        cleanup_airflow_pause || true
        return 1
    fi

    response="$(curl -sf --max-time 10 -X POST \
        -H "Authorization: Bearer ${airflow_token}" \
        -H 'Content-Type: application/json' \
        -d '{"logical_date":null}' \
        "http://127.0.0.1:${airflow_port}/api/v2/dags/${dag_id}/dagRuns" \
        2>/dev/null || true)"
    airflow_cleanup_run_id="$(jq -r '.dag_run_id // empty' <<<"$response")"
    if [[ -z "$airflow_cleanup_run_id" ]]; then
        fail "Airflow не создал ручной запуск пробника ${dag_id}"
        cleanup_airflow_pause || true
        return 1
    fi

    encoded_run_id="$(jq -rn --arg value "$airflow_cleanup_run_id" '$value | @uri')"
    for ((attempt = 1; attempt <= 60; attempt++)); do
        response="$(curl -sf --max-time 10 \
            -H "Authorization: Bearer ${airflow_token}" \
            "http://127.0.0.1:${airflow_port}/api/v2/dags/${dag_id}/dagRuns/${encoded_run_id}" \
            2>/dev/null || true)"
        run_state="$(jq -r '.state // empty' <<<"$response")"
        [[ "$run_state" == 'success' || "$run_state" == 'failed' ]] && break
        sleep 2
    done

    if [[ "$run_state" == 'success' ]] && \
        cleanup_airflow_run && cleanup_airflow_pause; then
        pass "пробник ${dag_id} завершился успешно, запуск удалён, исходная пауза восстановлена"
        return 0
    fi

    fail "пробник ${dag_id} не завершился чисто: состояние ${run_state:-неизвестно}"
    cleanup_airflow_run || true
    cleanup_airflow_pause || true
    return 1
}

check_airflow_probes() {
    if ! airflow_sign_in; then
        fail 'Airflow не принял учётные данные администратора, пробники не запускались'
        return
    fi
    run_airflow_probe test_clickhouse || true
    run_airflow_probe test_kafka || true
}

check_superset() {
    local config
    local login
    local metadata_tables
    local password
    local port
    local metadata_engine
    local stored_config
    local stored_uri
    local stored_uuid
    local superset_password
    local superset_user
    local user

    config="$(compose config --format json 2>/dev/null || true)"
    user="$(jq -r '.services.superset.environment.SUPERSET_ADMIN_USER // empty' <<<"$config")"
    password="$(jq -r '.services.superset.environment.SUPERSET_ADMIN_PASSWORD // empty' <<<"$config")"
    superset_user="$(jq -r '.services["postgres-metadata"].environment.SUPERSET_METADATA_USER // empty' <<<"$config")"
    superset_password="$(jq -r '.services["postgres-metadata"].environment.SUPERSET_METADATA_PASSWORD // empty' <<<"$config")"
    port="$(published_port superset 8088)"

    login="$(curl -sf --max-time 10 -X POST \
        -H 'Content-Type: application/json' \
        -d "$(jq -cn --arg username "$user" --arg password "$password" \
            '{username: $username, password: $password, provider: "db", refresh: true}')" \
        "http://127.0.0.1:${port}/api/v1/security/login" 2>/dev/null || true)"
    if curl -sf --max-time 10 "http://127.0.0.1:${port}/health" >/dev/null 2>&1 && \
        jq -e '.access_token | length > 0' >/dev/null 2>&1 <<<"$login"; then
        pass 'интерфейс Superset отвечает и принимает подготовленные учётные данные администратора'
    else
        fail 'интерфейс Superset не отвечает или не принимает учётные данные администратора'
    fi

    metadata_tables="$(timeout 20s "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" \
        exec -T -e "PGPASSWORD=${superset_password}" postgres-metadata \
        psql -U "$superset_user" -d superset -tAc \
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ab_user';" \
        2>/dev/null || true)"
    stored_config="$(timeout 30s "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" \
        exec -T superset python -c \
        "from superset.app import create_app; app = create_app(); app.app_context().push(); from superset.extensions import db; from superset.models.core import Database; url = db.engine.url; database = db.session.query(Database).filter_by(database_name='ClickHouse').one(); print('SMOKE_METADATA=' + '|'.join([url.get_backend_name(), url.username or '', url.host or '', str(url.port or ''), url.database or ''])); print('SMOKE_UUID=' + str(database.uuid)); print('SMOKE_URI=' + database.sqlalchemy_uri_decrypted)" \
        2>/dev/null)"
    metadata_engine="$(sed -n 's/^SMOKE_METADATA=//p' <<<"$stored_config" | tail -n 1)"
    stored_uuid="$(sed -n 's/^SMOKE_UUID=//p' <<<"$stored_config" | tail -n 1)"
    stored_uri="$(sed -n 's/^SMOKE_URI=//p' <<<"$stored_config" | tail -n 1)"
    if [[ "$metadata_tables" == '1' ]] && \
        [[ "$metadata_engine" == "postgresql|${superset_user}|postgres-metadata|5432|superset" ]] && \
        [[ "$stored_uuid" == '4b8f2c6e-1d3a-4f5b-9c7d-2e8a1f0b3c5d' ]] && \
        [[ "$stored_uri" == 'clickhousedb://default@clickhouse-02:8123/default' ]]; then
        pass 'метаданные Superset живут в Postgres, подготовленное подключение указывает на clickhouse-02'
    else
        fail "Superset не подтвердил Postgres и подготовленное подключение к ноде 2: таблицы=${metadata_tables:-нет}, движок=${metadata_engine:-нет}, UUID=${stored_uuid:-нет}, URI=${stored_uri:-нет}"
        return
    fi

    if printf 'n\n' | timeout 60s "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" \
        exec -T superset superset test-db "$stored_uri" >/dev/null 2>&1; then
        pass 'Superset успешно проверил извлечённое из метаданных подключение ClickHouse'
    else
        fail 'Superset не смог проверить извлечённое из метаданных подключение ClickHouse'
    fi
}

# Зависимости машины здесь не проверка, а условие запуска: «на машине есть
# Docker и curl» — утверждение о собранном стенде, и считает его make smoke.
if require_host_dependencies; then
    check_kafka_from_host
    check_airflow_probes
    check_superset
    check_containers_survived
else
    fail 'проверки Kafka, Airflow и Superset пропущены без зависимостей машины'
fi

print_total
[[ "$failed" -eq 0 ]]
