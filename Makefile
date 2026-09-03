COMPOSE ?= docker compose
GENERATOR_DAY ?= 0
GENERATOR_LIMIT ?=
GENERATOR_SPEED ?=

# Цели корня — про стенд; проверки генератора — в `generator/Makefile`.
.PHONY: up down clean rebuild-storage ps logs smoke check-clickhouse check-services config-test lint generate-batch generate-live

# --- Жизнь стенда ---

# Compose дожидается инфраструктуры, Airflow — готового прикладного мира.
up:
	$(COMPOSE) up --detach --build --wait --wait-timeout 600 --remove-orphans
	COMPOSE_BIN="$(COMPOSE)" ./scripts/run-dag.sh world_initialize

down:
	$(COMPOSE) down --remove-orphans

# Полное удаление стенда, а не способ получить чистые данные перед проверкой.
# Граница операции и требование к обоснованию — в AGENTS.md.
clean:
	$(COMPOSE) down --volumes --remove-orphans

# Возврат прикладного мира к началу без удаления инфраструктурных томов.
rebuild-storage:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/run-dag.sh world_recreate

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --follow

# --- Проверки, которым нужен поднятый стенд ---

smoke:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/stand-smoke.sh

check-clickhouse:
	./scripts/check-clickhouse.sh

check-services:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/stand-services.sh

# --- Проверки, которым стенд не нужен ---

config-test:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/config-test.sh

# Пути названы вслух: без них ruff из корня прошёлся бы и по генератору, а у
# того своя дверь и свой конфиг. Версия закреплена, потому что лока в корне
# нет, а форматтер между версиями меняет вывод — иначе проверка однажды
# покраснела бы сама, без единой правки в репозитории.
# Проверка рендера быстрая, не требует стенда и ловит устаревший HTML.
lint:
	uvx ruff@0.16.1 check dags infra/superset docs/handbook/architecture-map
	uvx ruff@0.16.1 format --check dags infra/superset docs/handbook/architecture-map
	uv run docs/handbook/architecture-map/render.py --check

# --- Наполнение миром ---

generate-batch:
	$(COMPOSE) --profile generator run --rm generator batch --day "$(GENERATOR_DAY)" \
		$(if $(GENERATOR_LIMIT),--limit "$(GENERATOR_LIMIT)")

generate-live:
	$(COMPOSE) --profile generator run --rm generator live --day "$(GENERATOR_DAY)" \
		$(if $(GENERATOR_SPEED),--speed "$(GENERATOR_SPEED)")
