COMPOSE ?= docker compose

.PHONY: up down clean ps logs config-test lint typecheck test docs smoke smoke-cluster

up:
	$(COMPOSE) up --detach --build --wait --wait-timeout 600

down:
	$(COMPOSE) down --remove-orphans

clean:
	$(COMPOSE) down --volumes --remove-orphans

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --follow

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

smoke:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/stand-smoke.sh

smoke-cluster:
	./scripts/clickhouse-smoke.sh
