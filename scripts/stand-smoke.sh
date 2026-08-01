#!/usr/bin/env bash
set -uo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly COMPOSE_FILE="$ROOT_DIR/compose.yaml"
readonly ENV_EXAMPLE="$ROOT_DIR/.env.example"
read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN:-docker compose}"
readonly -a LONG_LIVED_SERVICES=(
    clickhouse-keeper clickhouse-01 clickhouse-02 kafka postgres-metadata
    airflow-apiserver airflow-scheduler airflow-dag-processor
    superset prometheus grafana
)

passed=0
failed=0
kafka_cleanup_image=''
kafka_cleanup_port=''
kafka_cleanup_topic=''
airflow_cleanup_port=''
airflow_cleanup_dag_id=''
airflow_cleanup_original_paused=''
airflow_cleanup_run_id=''
airflow_cleanup_token=''

compose() {
    "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" "$@"
}

pass() {
    passed=$((passed + 1))
    printf 'ЗЕЛЁНО: %s.\n' "$1"
}

fail() {
    failed=$((failed + 1))
    printf 'ОШИБКА: %s.\n' "$1" >&2
}

check_env_consistency() {
    local LC_ALL=C
    local content
    local default
    local depth
    local env_value
    local expression
    local found_closing
    local i
    local inner
    local joined
    local j
    local length
    local line
    local nested
    local variable
    local -A env_count=()
    local -A env_values=()
    local -a problems=()
    local -A used=()
    local -a expressions=()

    if [[ ! -r "$COMPOSE_FILE" || ! -r "$ENV_EXAMPLE" ]]; then
        fail 'compose.yaml или .env.example недоступны для чтения'
        return
    fi

    # Разбираем только подстановки Compose и отдельно пропускаем $$ для команд контейнера.
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
            variable="${BASH_REMATCH[1]}"
            env_count["$variable"]=$(( ${env_count[$variable]:-0} + 1 ))
            env_values["$variable"]="${BASH_REMATCH[2]}"
        fi
    done <"$ENV_EXAMPLE"

    content="$(<"$COMPOSE_FILE")"
    length="${#content}"
    for ((i = 0; i < length - 1; i++)); do
        if [[ "${content:i:2}" == '$$' ]]; then
            i=$((i + 1))
            continue
        fi
        if [[ "${content:i:2}" != '${' ]]; then
            continue
        fi

        depth=1
        found_closing=0
        nested=0
        for ((j = i + 2; j < length; j++)); do
            if [[ "${content:j:2}" == '${' ]]; then
                nested=1
                depth=$((depth + 1))
                j=$((j + 1))
            elif [[ "${content:j:1}" == '}' ]]; then
                depth=$((depth - 1))
                if [[ "$depth" -eq 0 ]]; then
                    expressions+=("${content:i:j-i+1}")
                    found_closing=1
                    i="$j"
                    break
                fi
            fi
        done
        if [[ "$found_closing" -eq 0 ]]; then
            problems+=("незакрытая подстановка у позиции ${i}")
            break
        fi
        if [[ "$nested" -eq 1 ]]; then
            problems+=("${expressions[-1]}: вложенные подстановки запрещены, используйте простой вид \${VAR:-значение}")
        fi
    done

    for expression in "${expressions[@]}"; do
        inner="${expression:2:${#expression}-3}"
        if [[ "$inner" =~ ^([A-Za-z_][A-Za-z0-9_]*):-(.*)$ ]]; then
            variable="${BASH_REMATCH[1]}"
            default="${BASH_REMATCH[2]}"
            used["$variable"]=1
            if [[ "${env_count[$variable]:-0}" -ne 1 ]]; then
                problems+=("${variable}: нужна ровно одна строка в .env.example")
                continue
            fi
            env_value="${env_values[$variable]}"
            if [[ "$env_value" != "$default" ]]; then
                problems+=("${variable}: значение '${env_value}' не равно '${default}'")
            fi
        else
            problems+=("${expression}: нет значения по умолчанию вида :-")
        fi
    done

    for variable in "${!env_count[@]}"; do
        if [[ -z "${used[$variable]+x}" ]]; then
            problems+=("${variable}: не используется в compose.yaml")
        fi
    done

    if [[ "${#problems[@]}" -eq 0 ]]; then
        pass '.env.example совпадает со всеми значениями по умолчанию compose.yaml'
    else
        printf -v joined '%s; ' "${problems[@]}"
        fail "расхождение .env.example и compose.yaml: ${joined%; }"
    fi
}

check_host_dependencies() {
    local command
    local -a missing=()

    for command in docker curl jq awk grep sed tail sleep timeout; do
        command -v "$command" >/dev/null 2>&1 || missing+=("$command")
    done
    if [[ "${#missing[@]}" -gt 0 ]]; then
        fail "на машине не хватает команд: ${missing[*]}"
        return 1
    fi
    if ! docker compose version >/dev/null 2>&1; then
        fail 'Docker Compose недоступен; установите модуль compose для Docker'
        return 1
    fi
    if ! docker info >/dev/null 2>&1; then
        fail 'Docker недоступен; запустите Docker и проверьте доступ к его сокету'
        return 1
    fi
    pass 'на машине доступны Docker, Docker Compose, curl, jq, awk, grep, sed, tail, sleep и timeout'
}

check_container_health() {
    local container_id
    local service="$1"
    local state

    container_id="$(compose ps --all --quiet "$service" 2>/dev/null || true)"
    if [[ -z "$container_id" ]]; then
        fail "сервис ${service} не создан"
        return
    fi
    state="$(docker inspect --format '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}нет проверки состояния{{end}}' "$container_id" 2>/dev/null || true)"
    if [[ "$state" == 'running/healthy' ]]; then
        pass "сервис ${service} запущен и здоров"
    else
        fail "сервис ${service} нездоров: ${state:-состояние неизвестно}"
    fi
}

published_port() {
    local binding
    local service="$1"
    local container_port="$2"
    binding="$(compose port "$service" "$container_port" 2>/dev/null || true)"
    printf '%s\n' "${binding##*:}"
}

kafka_cli() {
    timeout 30s docker run --rm --network host "$kafka_cleanup_image" \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server "localhost:${kafka_cleanup_port}" "$@"
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
        -H "Authorization: Bearer ${airflow_cleanup_token}" \
        "http://127.0.0.1:${airflow_cleanup_port}/api/v2/dags/${airflow_cleanup_dag_id}/dagRuns/${encoded_run_id}" \
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
        -H "Authorization: Bearer ${airflow_cleanup_token}" \
        -H 'Content-Type: application/json' \
        -d "$payload" \
        "http://127.0.0.1:${airflow_cleanup_port}/api/v2/dags/${airflow_cleanup_dag_id}" \
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

    kafka_cleanup_image="$(compose config --format json | jq -r '.services.kafka.image')"
    kafka_cleanup_port="$(published_port kafka 29092)"
    kafka_cleanup_topic="stand_smoke_${EPOCHSECONDS}_$$_${RANDOM}"

    if [[ -n "$kafka_cleanup_port" ]] && \
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
        pass "Kafka доступна с машины через localhost:${kafka_cleanup_port}: временный топик создан, найден и удалён"
        return
    fi
    cleanup_kafka_topic || true
    fail "Kafka недоступна с машины через отображённый порт ${kafka_cleanup_port:-не найден} или временный топик не исчез после удаления"
}

check_prometheus_targets() {
    local port
    local response
    local targets_response

    port="$(published_port prometheus 9090)"
    response="$(curl -sf "http://127.0.0.1:${port}/api/v1/query?query=up" 2>/dev/null || true)"
    targets_response="$(curl -sf "http://127.0.0.1:${port}/api/v1/targets?state=active" 2>/dev/null || true)"
    if jq -e '
        .status == "success" and
        (.data.result | length == 3) and
        ([.data.result[].metric.instance] | sort ==
          ["clickhouse-01:9363", "clickhouse-02:9363", "clickhouse-keeper:9363"]) and
        all(.data.result[]; .value[1] == "1")
    ' >/dev/null 2>&1 <<<"$response" && jq -e '
        .status == "success" and
        (.data.activeTargets | length == 3) and
        ([.data.activeTargets[].scrapeUrl] | sort == [
          "http://clickhouse-01:9363/metrics",
          "http://clickhouse-02:9363/metrics",
          "http://clickhouse-keeper:9363/metrics"
        ]) and
        all(.data.activeTargets[]; .health == "up" and .lastError == "")
    ' >/dev/null 2>&1 <<<"$targets_response"; then
        pass 'Prometheus видит ровно три цели ClickHouse, все со значением up=1'
    else
        fail 'Prometheus не видит ровно три здоровые цели: clickhouse-01, clickhouse-02 и clickhouse-keeper'
    fi
}

check_grafana_datasource() {
    local config
    local password
    local port
    local datasource
    local provisioning_file_ok=0
    local response
    local user

    config="$(compose config --format json 2>/dev/null || true)"
    user="$(jq -r '.services.grafana.environment.GF_SECURITY_ADMIN_USER // empty' <<<"$config")"
    password="$(jq -r '.services.grafana.environment.GF_SECURITY_ADMIN_PASSWORD // empty' <<<"$config")"
    port="$(published_port grafana 3000)"
    timeout 20s "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" \
        exec -T grafana sh -ec '
            file=/etc/grafana/provisioning/datasources/prometheus.yml
            test -r "$file"
            grep -Eq "^[[:space:]]+uid: prometheus$" "$file"
            grep -Eq "^[[:space:]]+type: prometheus$" "$file"
            grep -Eq "^[[:space:]]+url: http://prometheus:9090$" "$file"
            grep -Eq "^[[:space:]]+isDefault: true$" "$file"
            grep -Eq "^[[:space:]]+editable: false$" "$file"
        ' >/dev/null 2>&1 && provisioning_file_ok=1
    datasource="$(curl -sf -u "${user}:${password}" \
        "http://127.0.0.1:${port}/api/datasources/uid/prometheus" 2>/dev/null || true)"
    response="$(curl -sf -u "${user}:${password}" \
        "http://127.0.0.1:${port}/api/datasources/uid/prometheus/health" 2>/dev/null || true)"
    if [[ "$provisioning_file_ok" -eq 1 ]] && jq -e '
        .uid == "prometheus" and
        .type == "prometheus" and
        .url == "http://prometheus:9090" and
        .access == "proxy" and
        .isDefault == true and
        .readOnly == true
    ' >/dev/null 2>&1 <<<"$datasource" && \
        jq -e '.status == "OK"' >/dev/null 2>&1 <<<"$response"; then
        pass 'Grafana проверила файл настройки и подготовленный источник Prometheus с uid=prometheus'
    else
        fail 'Grafana не смогла проверить файл настройки и подготовленный источник Prometheus с uid=prometheus'
    fi
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
        -H "Authorization: Bearer ${airflow_cleanup_token}" \
        "http://127.0.0.1:${airflow_cleanup_port}/api/v2/dags/${dag_id}" \
        2>/dev/null || true)"
    if ! jq -e --arg dag_id "$dag_id" '.dag_id == $dag_id' \
        >/dev/null 2>&1 <<<"$dag"; then
        fail "пробник ${dag_id} не виден через API Airflow"
        return 1
    fi

    airflow_cleanup_dag_id="$dag_id"
    airflow_cleanup_original_paused="$(jq -r '.is_paused | tostring' <<<"$dag")"
    unpaused="$(curl -sf --max-time 10 -X PATCH \
        -H "Authorization: Bearer ${airflow_cleanup_token}" \
        -H 'Content-Type: application/json' \
        -d '{"is_paused":false}' \
        "http://127.0.0.1:${airflow_cleanup_port}/api/v2/dags/${dag_id}" \
        2>/dev/null || true)"
    if ! jq -e '.is_paused == false' >/dev/null 2>&1 <<<"$unpaused"; then
        fail "Airflow не смог включить пробник ${dag_id} перед ручным запуском"
        cleanup_airflow_pause || true
        return 1
    fi

    response="$(curl -sf --max-time 10 -X POST \
        -H "Authorization: Bearer ${airflow_cleanup_token}" \
        -H 'Content-Type: application/json' \
        -d '{"logical_date":null}' \
        "http://127.0.0.1:${airflow_cleanup_port}/api/v2/dags/${dag_id}/dagRuns" \
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
            -H "Authorization: Bearer ${airflow_cleanup_token}" \
            "http://127.0.0.1:${airflow_cleanup_port}/api/v2/dags/${dag_id}/dagRuns/${encoded_run_id}" \
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

check_airflow() {
    local config
    local connection
    local dag
    local dag_id
    local health
    local password
    local response
    local user

    config="$(compose config --format json 2>/dev/null || true)"
    user="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_USER // empty' <<<"$config")"
    password="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_PASSWORD // empty' <<<"$config")"
    airflow_cleanup_port="$(published_port airflow-apiserver 8080)"

    health="$(curl -sf --max-time 10 \
        "http://127.0.0.1:${airflow_cleanup_port}/api/v2/monitor/health" 2>/dev/null || true)"
    if jq -e '
        .metadatabase.status == "healthy" and
        .scheduler.status == "healthy" and
        .dag_processor.status == "healthy"
    ' >/dev/null 2>&1 <<<"$health"; then
        pass 'API Airflow отвечает; база метаданных, планировщик и обработчик DAG здоровы'
    else
        fail 'API Airflow не подтвердил здоровье базы метаданных, планировщика и обработчика DAG'
    fi

    response="$(curl -sf --max-time 10 -X POST \
        -H 'Content-Type: application/json' \
        -d "$(jq -cn --arg username "$user" --arg password "$password" \
            '{username: $username, password: $password}')" \
        "http://127.0.0.1:${airflow_cleanup_port}/auth/token" 2>/dev/null || true)"
    airflow_cleanup_token="$(jq -r '.access_token // empty' <<<"$response")"
    if [[ -z "$airflow_cleanup_token" ]]; then
        fail 'Airflow не принял учётные данные администратора'
        return
    fi
    for dag_id in test_clickhouse test_kafka; do
        dag="$(curl -sf --max-time 10 \
            -H "Authorization: Bearer ${airflow_cleanup_token}" \
            "http://127.0.0.1:${airflow_cleanup_port}/api/v2/dags/${dag_id}" \
            2>/dev/null || true)"
        if ! jq -e --arg dag_id "$dag_id" '.dag_id == $dag_id' \
            >/dev/null 2>&1 <<<"$dag"; then
            fail "Airflow не показал пробник ${dag_id}"
            return
        fi
    done
    pass 'учётные данные администратора Airflow принимаются, оба пробника видны через API'

    connection="$(curl -sf --max-time 10 \
        -H "Authorization: Bearer ${airflow_cleanup_token}" \
        "http://127.0.0.1:${airflow_cleanup_port}/api/v2/connections/clickhouse_default" \
        2>/dev/null || true)"
    if jq -e '
        .connection_id == "clickhouse_default" and
        .host == "clickhouse-01" and
        .port == 8123 and
        .login == "default" and
        .schema == "default"
    ' >/dev/null 2>&1 <<<"$connection" && \
        timeout 20s "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" \
            exec -T airflow-scheduler \
            curl -sf 'http://clickhouse-01:8123/?query=SELECT%201' \
            2>/dev/null | grep -qx '1'; then
        pass 'подготовленное подключение Airflow указывает на clickhouse-01, нода доступна из контейнера'
    else
        fail 'подключение Airflow не указывает на clickhouse-01 или нода недоступна из контейнера'
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

# Убитый за память контейнер Docker поднимает сам, и проверка здоровья об этом
# промолчит. Порога здесь нет: это «да или нет», а не бюджет памяти (ADR 0004).
check_containers_survived() {
    local container_id
    local service
    local state
    local problems=0

    for service in "${LONG_LIVED_SERVICES[@]}"; do
        container_id="$(compose ps --all --quiet "$service" 2>/dev/null || true)"
        if [[ -z "$container_id" || "$container_id" == *$'\n'* ]]; then
            fail "не удалось получить контейнер ${service} для проверки перезапусков"
            return
        fi
        state="$(docker inspect --format '{{.State.OOMKilled}}/{{.RestartCount}}' "$container_id" 2>/dev/null || true)"
        case "$state" in
            false/0) ;;
            # OOMKilled встаёт и когда убит процесс внутри живого контейнера.
            true/*)
                fail "в контейнере ${service} ядро убило процесс из-за нехватки памяти"
                problems=$((problems + 1))
                ;;
            false/*)
                fail "контейнер ${service} перезапускался, счётчик Docker — ${state#*/}"
                problems=$((problems + 1))
                ;;
            *)
                fail "Docker не рассказал о состоянии контейнера ${service}"
                problems=$((problems + 1))
                ;;
        esac
    done
    if [[ "$problems" -eq 0 ]]; then
        pass 'ни в одном долгоживущем контейнере ядро не убивало процессы за память, и никто не перезапускался сам'
    fi
}

check_env_consistency
if check_host_dependencies; then
    for service in "${LONG_LIVED_SERVICES[@]}"; do
        check_container_health "$service"
    done
    check_kafka_from_host
    check_prometheus_targets
    check_grafana_datasource
    check_airflow
    check_superset
    check_containers_survived
else
    fail 'проверки контейнеров, Kafka, Airflow, Superset, Prometheus и Grafana пропущены без зависимостей машины'
fi

printf 'ИТОГ: пройдено %d, ошибок %d\n' "$passed" "$failed"
[[ "$failed" -eq 0 ]]
