#!/usr/bin/env bash
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN:-docker compose}"
readonly CACHE_DIR="$(mktemp -d)"
readonly SHELL_FILES_LIST="$CACHE_DIR/shell-files"

cleanup() {
    rm -rf "$CACHE_DIR"
}

trap cleanup EXIT

"${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" config --quiet
config_json="$("${COMPOSE_CMD[@]}" --project-directory "$ROOT_DIR" config --format json)"
grep -qx 'ARG SUPERSET_BASE_IMAGE' "$ROOT_DIR/infra/superset/Dockerfile"
jq -e '
    .services.superset.image == "clickstream-superset:local" and
    (.services.superset.build.args.SUPERSET_BASE_IMAGE | length > 0)
' >/dev/null <<<"$config_json"
grep -qx 'ARG AIRFLOW_BASE_IMAGE' "$ROOT_DIR/infra/airflow/Dockerfile"
jq -e '
    .services["airflow-init"].image == "clickstream-airflow:local" and
    (.services["airflow-init"].build.args.AIRFLOW_BASE_IMAGE | length > 0) and
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
if ! find "$ROOT_DIR" \
    -path "$ROOT_DIR/.git" -prune -o \
    -type f -name '*.sh' -print0 >"$SHELL_FILES_LIST"; then
    printf 'ОШИБКА: не удалось получить список файлов Bash для проверки.\n' >&2
    exit 1
fi
mapfile -d '' -t shell_files <"$SHELL_FILES_LIST"
if [[ "${#shell_files[@]}" -eq 0 ]]; then
    printf 'ОШИБКА: не найдено ни одного файла Bash для проверки.\n' >&2
    exit 1
fi
bash -n "${shell_files[@]}"
PYTHONPYCACHEPREFIX="$CACHE_DIR" uv run --no-project python -m compileall -q "$ROOT_DIR/dags"
git -C "$ROOT_DIR" diff --check

printf 'ЗЕЛЁНО: Compose, Bash, Python и пробельные ошибки diff проверены.\n'
