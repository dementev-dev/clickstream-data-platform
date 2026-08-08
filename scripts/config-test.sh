#!/usr/bin/env bash
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN:-docker compose}"
readonly CACHE_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "$CACHE_DIR"
}

trap cleanup EXIT

"${COMPOSE_CMD[@]}" \
    --project-directory "$ROOT_DIR" \
    --env-file "$ROOT_DIR/.env.example" \
    config --quiet
config_json="$(
    "${COMPOSE_CMD[@]}" \
        --project-directory "$ROOT_DIR" \
        --env-file "$ROOT_DIR/.env.example" \
        config --format json
)"
# Контекст сборки образов — корень репозитория, и локальный `.env` с настоящими
# паролями уехал бы в слой образа молча. Это единственная здешняя ошибка, о
# которой никто не узнает, пока образ не окажется у чужого.
for private_path in '.env' '.env.*' '*.pem' '*.key' '*.crt' 'secrets/' 'credentials/'; do
    grep -qxF "$private_path" "$ROOT_DIR/.dockerignore"
done
jq -e '
    .services.superset.image == "clickstream-superset:local"
' >/dev/null <<<"$config_json"
jq -e '
    .services["airflow-init"].image == "clickstream-airflow:local" and
    all(
        .services[];
        ((.environment // {}) | has("_PIP_ADDITIONAL_REQUIREMENTS") | not)
    )
' >/dev/null <<<"$config_json"
jq -e '
    .services["airflow-init"] as $service |
    ($service.environment.AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE ==
      "/opt/airflow/auth/simple_auth_manager_passwords.json") and
    any($service.volumes[]; .source == "airflow_auth" and .target == "/opt/airflow/auth") and
    any($service.volumes[]; .source == "airflow_logs" and .target == "/opt/airflow/logs")
' >/dev/null <<<"$config_json"
# Файлы DAG на машине никто не запускает: их разбирает обработчик внутри
# контейнера, и синтаксическая ошибка там всплывает не сообщением, а тем, что
# DAG молча не появился в списке. Локальный разбор — единственная дешёвая
# обратная связь. Скрипты стенда проверять так незачем: их запускают с этой же
# машины, и ошибка вылезает при первом же запуске с номером строки.
PYTHONPYCACHEPREFIX="$CACHE_DIR" uv run --no-project python -m compileall -q "$ROOT_DIR/dags"
git -C "$ROOT_DIR" diff --check

printf 'ЗЕЛЁНО: Compose, Python и пробельные ошибки diff проверены.\n'
