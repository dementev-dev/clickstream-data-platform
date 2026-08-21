#!/usr/bin/env bash
# Запускает публичный даг Airflow и возвращает его итог вызывающему make.
set -Eeuo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly DAG_ID="${1:-}"
# Каждый цикл ограничен двадцатью минутами: успешная пересборка заняла 3 м 9 с.
readonly ATTEMPTS=400
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

next_step() {
    case "$DAG_ID" in
        world_initialize)
            printf 'посмотрите журнал; повторите make up, а для недействительного мира — make rebuild-storage'
            ;;
        world_recreate) printf 'устраните причину и повторите make rebuild-storage' ;;
        *) printf 'проверьте журнал дага и повторите команду' ;;
    esac
}

[[ -n "$DAG_ID" ]] || fail 'укажите идентификатор дага'
[[ $# -eq 1 ]] || fail 'ожидался один аргумент: идентификатор дага'

config="$(compose config --format json)"
user="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_USER // empty' <<<"$config")"
password="$(jq -r '.services["airflow-apiserver"].environment.AIRFLOW_ADMIN_PASSWORD // empty' <<<"$config")"
binding="$(compose port airflow-apiserver 8080 2>/dev/null || true)"
port="${binding##*:}"
[[ -n "$user" && -n "$password" ]] || fail 'не найдены учётные данные Airflow в Compose'
[[ -n "$port" ]] || fail 'API Airflow не запущен; сначала поднимите инфраструктуру'

# В API v2 logical_date=null означает событийный ручной запуск. Даты мира уже
# записаны в данных, календарь Airflow здесь не должен придумывать свою.
if ! response="$(curl -sS --fail-with-body --max-time 10 -X POST \
    -H 'Content-Type: application/json' \
    -d "$(jq -cn --arg username "$user" --arg password "$password" \
        '{username: $username, password: $password}')" \
    "http://127.0.0.1:${port}/auth/token" 2>/dev/null)"; then
    fail 'Airflow не принял учётные данные администратора'
fi
token="$(jq -r '.access_token // empty' <<<"$response")"
[[ -n "$token" ]] || fail 'Airflow не вернул токен доступа'

# Здоровый обработчик DAG ещё не означает, что новый файл уже появился в API.
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
    fail "даг ${DAG_ID} не появился за $((ATTEMPTS * PAUSE_SECONDS)) с; $(next_step)"

if ! response="$(curl -sS --fail-with-body --max-time 10 -X POST \
    -H "Authorization: Bearer ${token}" \
    -H 'Content-Type: application/json' \
    -d '{"logical_date":null}' \
    "http://127.0.0.1:${port}/api/v2/dags/${DAG_ID}/dagRuns" 2>/dev/null)"; then
    detail="$(jq -r '.detail // .message // empty' <<<"$response" 2>/dev/null || true)"
    fail "Airflow не запустил даг ${DAG_ID}${detail:+: ${detail}}; $(next_step)"
fi
run_id="$(jq -r '.dag_run_id // empty' <<<"$response")"
state="$(jq -r '.state // "queued"' <<<"$response")"
[[ -n "$run_id" ]] || fail "Airflow не вернул идентификатор запуска ${DAG_ID}"

encoded_run_id="$(jq -rn --arg value "$run_id" '$value | @uri')"
printf 'Airflow: даг %s, запуск %s, состояние %s.\n' "$DAG_ID" "$run_id" "$state"

for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    response="$(curl -sf --max-time 10 \
        -H "Authorization: Bearer ${token}" \
        "http://127.0.0.1:${port}/api/v2/dags/${DAG_ID}/dagRuns/${encoded_run_id}" \
        2>/dev/null || true)"
    state="$(jq -r '.state // empty' <<<"$response" 2>/dev/null || true)"
    case "$state" in
        success)
            printf 'ЗЕЛЁНО: даг %s, запуск %s, состояние success.\n' "$DAG_ID" "$run_id"
            exit 0
            ;;
        failed)
            fail "даг ${DAG_ID}, запуск ${run_id}, состояние ${state}; $(next_step)"
            ;;
    esac
    if ((attempt % REPORT_EVERY == 0)); then
        printf 'Airflow: даг %s, запуск %s, состояние %s; ждём (%d с).\n' \
            "$DAG_ID" "$run_id" "${state:-неизвестно}" "$((attempt * PAUSE_SECONDS))"
    fi
    sleep "$PAUSE_SECONDS"
done

fail "даг ${DAG_ID}, запуск ${run_id}, состояние ${state:-неизвестно} не завершился за $((ATTEMPTS * PAUSE_SECONDS)) с; $(next_step)"
