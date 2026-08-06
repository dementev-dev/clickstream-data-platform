#!/usr/bin/env bash
# Общая часть проверок на поднятом стенде: счёт проверок, обращение к Compose,
# зависимости машины и вопрос Docker, пережили ли контейнеры прогон. Файл не
# запускается сам — его подключают через source из stand-smoke.sh и
# stand-services.sh.

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN:-docker compose}"
readonly -a LONG_LIVED_SERVICES=(
    clickhouse-keeper clickhouse-01 clickhouse-02 kafka postgres-metadata
    airflow-apiserver airflow-scheduler airflow-dag-processor
    superset prometheus grafana
)

passed=0
failed=0

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

published_port() {
    local binding
    local service="$1"
    local container_port="$2"
    binding="$(compose port "$service" "$container_port" 2>/dev/null || true)"
    printf '%s\n' "${binding##*:}"
}

# Отвечает, есть ли на машине всё, без чего проверкам не с чем работать.
# Засчитывать ли этот ответ проверкой, решает вызывающий: смоук печатает за него
# ЗЕЛЁНО, потому что вопрос к машине стоит рядом с «стенд собран»; для
# check-services это условие запуска, и своего ЗЕЛЁНО у него там нет.
require_host_dependencies() {
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
    return 0
}

# Убитый за память контейнер Docker поднимает сам, и проверка здоровья об этом
# промолчит. Порога здесь нет: это «да или нет», а не бюджет памяти (ADR 0004).
# Проверка стоит в конце обеих целей: нагружает стенд check-services, а увидеть
# последствия нужно и тому, кто гонял один смоук.
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

# Время печатается как замер, а не как порог: смотрит на него человек.
print_total() {
    printf 'ИТОГ: пройдено %d, ошибок %d, время %d с\n' "$passed" "$failed" "$SECONDS"
}
