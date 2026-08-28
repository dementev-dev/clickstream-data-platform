.PHONY: lint typecheck test demo docs inventory

# --- Проверки ---

lint:
	uv run ruff check
	uv run ruff format --check

typecheck:
	uv run ty check

test:
	uv run pytest

# --- Пример из путеводителя ---

demo:
	uv run python -m clickstream_generator.demo

# --- Сборка из кода ---

docs:
	uv run python -m clickstream_generator.schema_doc \
		../docs/formats/clickstream-event.md

inventory:
	uv run python -m clickstream_generator.inventory \
		../data/world-inventory.json
