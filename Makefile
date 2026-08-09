COMPOSE ?= docker compose
GENERATOR_DAY ?= 0
GENERATOR_LIMIT ?=
GENERATOR_SPEED ?=

# Цели корня — про стенд; проверки генератора — в `generator/Makefile`.
.PHONY: up down clean ps logs smoke check-clickhouse check-services config-test lint generate-batch generate-live

# --- Жизнь стенда ---

# Второй шаг: `--wait` дожидается служб, а приём событий асинхронный — довод
# целиком в шапке скрипта.
up:
	$(COMPOSE) up --detach --build --wait --wait-timeout 600
	COMPOSE_BIN="$(COMPOSE)" ./scripts/wait-for-world.sh

down:
	$(COMPOSE) down --remove-orphans

clean:
	$(COMPOSE) down --volumes --remove-orphans

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
lint:
	uvx ruff@0.16.1 check dags infra/superset
	uvx ruff@0.16.1 format --check dags infra/superset

# --- Наполнение миром ---

generate-batch:
	$(COMPOSE) --profile generator run --rm generator batch --day "$(GENERATOR_DAY)" \
		$(if $(GENERATOR_LIMIT),--limit "$(GENERATOR_LIMIT)")

generate-live:
	$(COMPOSE) --profile generator run --rm generator live --day "$(GENERATOR_DAY)" \
		$(if $(GENERATOR_SPEED),--speed "$(GENERATOR_SPEED)")
