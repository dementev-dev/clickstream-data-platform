COMPOSE := docker compose

.PHONY: up down clean ps logs smoke smoke-guards

up:
	$(COMPOSE) up --detach --wait --wait-timeout 180

down:
	$(COMPOSE) down --remove-orphans

clean:
	$(COMPOSE) down --volumes --remove-orphans

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --follow

smoke:
	./scripts/clickhouse-smoke.sh

smoke-guards:
	./tests/smoke-guards.sh
