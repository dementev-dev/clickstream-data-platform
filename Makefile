COMPOSE ?= docker compose
GENERATOR_DAY ?= 0
GENERATOR_LIMIT ?=
GENERATOR_SPEED ?=

.PHONY: up down clean ps logs generate-batch generate-live config-test lint typecheck test docs inventory smoke check-clickhouse check-services

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

generate-batch:
	$(COMPOSE) --profile generator run --rm generator batch --day "$(GENERATOR_DAY)" \
		$(if $(GENERATOR_LIMIT),--limit "$(GENERATOR_LIMIT)")

generate-live:
	$(COMPOSE) --profile generator run --rm generator live --day "$(GENERATOR_DAY)" \
		$(if $(GENERATOR_SPEED),--speed "$(GENERATOR_SPEED)")

config-test:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/config-test.sh

lint:
	cd generator && uv run ruff check && uv run ruff format --check

typecheck:
	cd generator && uv run ty check

test:
	cd generator && uv run pytest

docs:
	cd generator && uv run python -m clickstream_generator.schema_doc \
		../docs/formats/clickstream-event.md

inventory:
	cd generator && uv run python -m clickstream_generator.inventory \
		../data/world-inventory.json

smoke:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/stand-smoke.sh

check-clickhouse:
	./scripts/check-clickhouse.sh

check-services:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/stand-services.sh
