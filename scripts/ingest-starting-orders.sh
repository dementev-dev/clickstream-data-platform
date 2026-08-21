#!/usr/bin/env bash
# Последний шаг `make up`: запускает единственный путь приёма заказов и ждёт
# его конца. Слепки к этому моменту уже лежат в Kafka, а события доехали до ODS.
set -Eeuo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly DAG_ID='orders_ingest'
# Потолок каждого ожидания — 300 с: больше чем всемеро от замеренных 40 с
# между запуском сценария и концом дага (#96). Это предел, а не бюджет времени.
readonly ATTEMPTS=100
readonly PAUSE_SECONDS=3
readonly REPORT_EVERY=5

read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN:-docker compose}"

fail() {
    printf 'ОШИБКА: %s\n' "$1" >&2
    exit 1
}

compose() {
    "${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" "$@"
}

config="$(compose config --format json)"
user="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_USER // empty' <<<"$config")"
password="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_PASSWORD // empty' <<<"$config")"
binding="$(compose port airflow-apiserver 8080)"
port="${binding##*:}"
[[ -n "$port" ]] || fail 'не найден отображённый порт API Airflow'

response="$(curl -sf --max-time 10 -X POST \
    -H 'Content-Type: application/json' \
    -d "$(jq -cn --arg username "$user" --arg password "$password" \
        '{username: $username, password: $password}')" \
    "http://127.0.0.1:${port}/auth/token" 2>/dev/null || true)"
token="$(jq -r '.access_token // empty' <<<"$response")"
[[ -n "$token" ]] || fail 'Airflow не принял учётные данные администратора'

# После `up --wait` обработчик DAG здоров, но файл мог ещё не попасть в список.
# Ограниченный опрос закрывает эту гонку без паузы наугад.
dag=''
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    dag="$(curl -sf --max-time 10 \
        -H "Authorization: Bearer ${token}" \
        "http://127.0.0.1:${port}/api/v2/dags/${DAG_ID}" \
        2>/dev/null || true)"
    jq -e --arg dag_id "$DAG_ID" '.dag_id == $dag_id' \
        >/dev/null 2>&1 <<<"$dag" && break
    sleep "$PAUSE_SECONDS"
done
jq -e --arg dag_id "$DAG_ID" '.dag_id == $dag_id' \
    >/dev/null 2>&1 <<<"$dag" || \
    fail "даг ${DAG_ID} не появился в Airflow за $((ATTEMPTS * PAUSE_SECONDS)) с"

# В API v2 Airflow 3.3 logical_date=null означает событийный ручной запуск.
# Это уместно для дага без расписания: календарь Airflow не подменяет дату
# слепка, которая уже записана в сообщениях.
response="$(curl -sf --max-time 10 -X POST \
    -H "Authorization: Bearer ${token}" \
    -H 'Content-Type: application/json' \
    -d '{"logical_date":null}' \
    "http://127.0.0.1:${port}/api/v2/dags/${DAG_ID}/dagRuns" \
    2>/dev/null || true)"
run_id="$(jq -r '.dag_run_id // empty' <<<"$response")"
[[ -n "$run_id" ]] || fail "Airflow не запустил даг ${DAG_ID}"

encoded_run_id="$(jq -rn --arg value "$run_id" '$value | @uri')"
printf 'Ждём приём стартовых слепков дагом %s...\n' "$DAG_ID"
state=''
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    response="$(curl -sf --max-time 10 \
        -H "Authorization: Bearer ${token}" \
        "http://127.0.0.1:${port}/api/v2/dags/${DAG_ID}/dagRuns/${encoded_run_id}" \
        2>/dev/null || true)"
    state="$(jq -r '.state // empty' <<<"$response")"
    case "$state" in
        success)
            printf 'ЗЕЛЁНО: стартовые слепки приняты в ODS.\n'
            exit 0
            ;;
        failed)
            fail "даг ${DAG_ID} завершился с ошибкой; запуск ${run_id}"
            ;;
    esac
    if ((attempt % REPORT_EVERY == 0)); then
        printf 'Приём ещё не завершён: состояние %s, ждём дальше (%d с)...\n' \
            "${state:-неизвестно}" "$((attempt * PAUSE_SECONDS))"
    fi
    sleep "$PAUSE_SECONDS"
done

fail "даг ${DAG_ID} не завершился за $((ATTEMPTS * PAUSE_SECONDS)) с:
  состояние ${state:-неизвестно}, запуск ${run_id}"
