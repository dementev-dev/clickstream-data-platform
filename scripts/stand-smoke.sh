#!/usr/bin/env bash
# make smoke — стенд собран: службы живы, порты отвечают, подключения настроены
# друг на друга. Проверка идёт вширь и по касательной к каждой службе, ждать
# службу здесь не полагается. Карта целей и правило быстрого смоука —
# docs/architecture/testing.md.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/stand-common.sh" || exit 1

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

keeper_exec() {
    timeout 20s "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" \
        exec -T clickhouse-keeper "$@" 2>/dev/null
}

# Три настройки keeper объявлены в compose.yaml: свой пользователь, свой предел
# на открытые файлы и свой том под /var/lib/clickhouse. Проверка спрашивает у
# живого контейнера, дошли ли они до процесса — здоровым keeper выглядит и без
# них, а каталог координации, однажды созданный от чужого пользователя,
# следующий запуск уже не откроет.
check_keeper_runtime() {
    local keeper_nofile
    local keeper_owner
    local keeper_user

    keeper_user="$(keeper_exec id -un)"
    keeper_nofile="$(keeper_exec \
        awk '$1 == "Max" && $2 == "open" && $3 == "files" {print $4}' /proc/1/limits)"
    keeper_owner="$(keeper_exec stat -c '%U:%G' /var/lib/clickhouse/coordination)"
    if [[ "$keeper_user" == 'clickhouse' ]] && \
        [[ "$keeper_nofile" -ge 262144 ]] && \
        [[ "$keeper_owner" == 'clickhouse:clickhouse' ]]; then
        pass 'keeper работает от clickhouse с nofile 262144 и своим каталогом данных'
    else
        fail "неверное окружение keeper: пользователь=${keeper_user:-нет ответа}, nofile=${keeper_nofile:-нет ответа}, владелец каталога=${keeper_owner:-нет ответа}"
    fi
}

# Самая известная поломка Kafka — объявленный адрес не совпадает с тем, по
# которому к брокеру стучатся снаружи. Выглядит она издевательски: клиент
# подключается, получает метаданные и виснет на адресе, которого с его стороны
# не существует. Проверка состояния контейнера этого не увидит — она спрашивает
# брокер изнутри и по внутреннему слушателю (compose.yaml, healthcheck kafka).
# Здесь запрос идёт с машины через отображённый порт, и списка топиков клиент не
# получит, не сходив вторым шагом по объявленному адресу. Отсюда и цена: почти
# вся она — старт JVM в разовом контейнере, а не разговор с брокером.
check_kafka_external_listener() {
    local image
    local port

    image="$(compose config --format json 2>/dev/null | jq -r '.services.kafka.image // empty')"
    port="$(published_port kafka 29092)"
    if [[ -n "$image" ]] && [[ -n "$port" ]] && \
        timeout 15s docker run --rm --network host "$image" \
            /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server "localhost:${port}" --list >/dev/null 2>&1; then
        pass "Kafka отвечает с машины через localhost:${port}: объявленный адрес ведёт туда же, куда отображён порт"
    else
        fail "Kafka не ответила с машины через localhost:${port:-порт не найден}: закрыт внешний слушатель, не отображён порт или объявленный адрес ведёт не туда"
    fi
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
        (.data.result | length == 4) and
        ([.data.result[].metric.instance] | sort ==
          ["clickhouse-01:9363", "clickhouse-02:9363", "clickhouse-keeper:9363", "kafka-exporter:9308"]) and
        all(.data.result[]; .value[1] == "1")
    ' >/dev/null 2>&1 <<<"$response" && jq -e '
        .status == "success" and
        (.data.activeTargets | length == 4) and
        ([.data.activeTargets[].scrapeUrl] | sort == [
          "http://clickhouse-01:9363/metrics",
          "http://clickhouse-02:9363/metrics",
          "http://clickhouse-keeper:9363/metrics",
          "http://kafka-exporter:9308/metrics"
        ]) and
        all(.data.activeTargets[]; .health == "up" and .lastError == "")
    ' >/dev/null 2>&1 <<<"$targets_response"; then
        pass 'Prometheus видит ровно четыре здоровые цели ClickHouse и Kafka'
    else
        fail 'Prometheus не видит ровно четыре здоровые цели: clickhouse-01, clickhouse-02, clickhouse-keeper и kafka-exporter'
    fi
}

check_grafana_datasource() {
    local clickhouse_datasource
    local clickhouse_response
    local config
    local datasources
    local password
    local port
    local provisioning_file_ok=0
    local prometheus_datasource
    local prometheus_response
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
            file=/etc/grafana/provisioning/datasources/clickhouse.yml
            test -r "$file"
            grep -Eq "^[[:space:]]+uid: clickhouse$" "$file"
            grep -Eq "^[[:space:]]+type: grafana-clickhouse-datasource$" "$file"
            grep -Eq "^[[:space:]]+host: clickhouse-01$" "$file"
            grep -Eq "^[[:space:]]+port: 9000$" "$file"
            grep -Eq "^[[:space:]]+username: grafana$" "$file"
            grep -Eq "^[[:space:]]+editable: false$" "$file"
        ' >/dev/null 2>&1 && provisioning_file_ok=1
    datasources="$(curl -sf -u "${user}:${password}" \
        "http://127.0.0.1:${port}/api/plugins?type=datasource" 2>/dev/null || true)"
    prometheus_datasource="$(curl -sf -u "${user}:${password}" \
        "http://127.0.0.1:${port}/api/datasources/uid/prometheus" 2>/dev/null || true)"
    prometheus_response="$(curl -sf -u "${user}:${password}" \
        "http://127.0.0.1:${port}/api/datasources/uid/prometheus/health" 2>/dev/null || true)"
    clickhouse_datasource="$(curl -sf -u "${user}:${password}" \
        "http://127.0.0.1:${port}/api/datasources/uid/clickhouse" 2>/dev/null || true)"
    clickhouse_response="$(curl -sf -u "${user}:${password}" \
        "http://127.0.0.1:${port}/api/datasources/uid/clickhouse/health" 2>/dev/null || true)"
    if [[ "$provisioning_file_ok" -ne 1 ]]; then
        fail 'Grafana не получила файлы настройки источников'
        return
    fi
    if ! jq -e '
        any(.[]; .id == "grafana-clickhouse-datasource")
    ' >/dev/null 2>&1 <<<"$datasources"; then
        fail 'Grafana не видит плагин ClickHouse'
        return
    fi
    if ! jq -e '
        .uid == "prometheus" and
        .type == "prometheus" and
        .url == "http://prometheus:9090" and
        .access == "proxy" and
        .isDefault == true and
        .readOnly == true
    ' >/dev/null 2>&1 <<<"$prometheus_datasource" || \
        ! jq -e '.status == "OK"' >/dev/null 2>&1 <<<"$prometheus_response"; then
        fail 'Grafana не видит здоровый подготовленный источник Prometheus'
        return
    fi
    if ! jq -e '
        .uid == "clickhouse" and
        .type == "grafana-clickhouse-datasource" and
        .access == "proxy" and
        .isDefault == false and
        .readOnly == true and
        .jsonData.host == "clickhouse-01" and
        .jsonData.port == 9000 and
        .jsonData.protocol == "native" and
        .jsonData.username == "grafana" and
        .jsonData.defaultDatabase == "system"
    ' >/dev/null 2>&1 <<<"$clickhouse_datasource" || \
        ! jq -e '.status == "OK"' >/dev/null 2>&1 <<<"$clickhouse_response"; then
        fail 'Grafana не видит здоровый подготовленный источник ClickHouse'
        return
    fi
    pass 'Grafana видит плагин и два здоровых подготовленных источника'
}

# Три вопроса о связности, и все три Airflow отвечает сразу. Запуск пробников —
# другое дело: он ждёт такта планировщика и живёт в make check-services.
check_airflow() {
    local airflow_port
    local airflow_token
    local clickhouse_password
    local config
    local connection
    local dag
    local dag_id
    local health
    local lifecycle_connection
    local password
    local response
    local user

    config="$(compose config --format json 2>/dev/null || true)"
    user="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_USER // empty' <<<"$config")"
    password="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_PASSWORD // empty' <<<"$config")"
    clickhouse_password="$(jq -r '.services["airflow-scheduler"].environment.CLICKHOUSE_ETL_PASSWORD // empty' <<<"$config")"
    airflow_port="$(published_port airflow-apiserver 8080)"

    health="$(curl -sf --max-time 10 \
        "http://127.0.0.1:${airflow_port}/api/v2/monitor/health" 2>/dev/null || true)"
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
        "http://127.0.0.1:${airflow_port}/auth/token" 2>/dev/null || true)"
    airflow_token="$(jq -r '.access_token // empty' <<<"$response")"
    if [[ -z "$airflow_token" ]]; then
        fail 'Airflow не принял учётные данные администратора'
        return
    fi
    for dag_id in test_clickhouse test_kafka; do
        dag="$(curl -sf --max-time 10 \
            -H "Authorization: Bearer ${airflow_token}" \
            "http://127.0.0.1:${airflow_port}/api/v2/dags/${dag_id}" \
            2>/dev/null || true)"
        if ! jq -e --arg dag_id "$dag_id" '.dag_id == $dag_id' \
            >/dev/null 2>&1 <<<"$dag"; then
            fail "Airflow не показал пробник ${dag_id}"
            return
        fi
    done
    pass 'учётные данные администратора Airflow принимаются, оба пробника видны через API'

    connection="$(curl -sf --max-time 10 \
        -H "Authorization: Bearer ${airflow_token}" \
        "http://127.0.0.1:${airflow_port}/api/v2/connections/clickhouse_default" \
        2>/dev/null || true)"
    lifecycle_connection="$(curl -sf --max-time 10 \
        -H "Authorization: Bearer ${airflow_token}" \
        "http://127.0.0.1:${airflow_port}/api/v2/connections/clickhouse_lifecycle" \
        2>/dev/null || true)"
    if jq -e '
        .connection_id == "clickhouse_default" and
        .conn_type == "clickhouse" and
        .host == "clickhouse-01" and
        .port == 8123 and
        .login == "etl" and
        .schema == "default"
    ' >/dev/null 2>&1 <<<"$connection" && jq -e '
        .connection_id == "clickhouse_lifecycle" and
        .conn_type == "clickhouse" and
        .host == "clickhouse-01" and
        .port == 8123 and
        .login == "lifecycle" and
        .schema == "default"
    ' >/dev/null 2>&1 <<<"$lifecycle_connection" && \
        timeout 20s "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" \
            exec -T airflow-scheduler \
            curl -sf -u "etl:${clickhouse_password}" \
            'http://clickhouse-01:8123/?query=SELECT%201' \
            2>/dev/null | grep -qx '1'; then
        pass 'оба подключения Airflow имеют тип clickhouse, нода 1 доступна из контейнера'
    else
        fail 'тип или адрес подключений Airflow отличается от ожидаемого либо нода 1 недоступна из контейнера'
    fi
}

if require_host_dependencies; then
    pass 'на машине доступны Docker, Docker Compose, curl, jq, awk, grep, sed, tail, sleep и timeout'
    for service in "${LONG_LIVED_SERVICES[@]}"; do
        check_container_health "$service"
    done
    check_keeper_runtime
    check_kafka_external_listener
    check_prometheus_targets
    check_grafana_datasource
    check_airflow
    check_containers_survived
else
    fail 'проверки контейнеров, Airflow, Prometheus и Grafana пропущены без зависимостей машины'
fi

print_total
[[ "$failed" -eq 0 ]]
