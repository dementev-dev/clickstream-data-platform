COMPOSE ?= docker compose

.PHONY: up down clean ps logs config-test smoke smoke-cluster smoke-guards

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
	./tests/stand-smoke-static.sh
	./tests/docs-guards.sh

smoke:
	COMPOSE_BIN="$(COMPOSE)" ./scripts/stand-smoke.sh

smoke-cluster:
	./scripts/clickhouse-smoke.sh

smoke-guards:
	./tests/smoke-guards.sh
	COMPOSE_BIN="$(COMPOSE)" ./tests/stand-smoke-guards.sh
