# Цели генератора. Дверь у него своя, потому что своя и сущность: пакет с
# собственными `pyproject.toml`, `uv.lock` и образом, заменяемый целиком
# (спека генератора, раздел 1). Отсюда и короткие имена — каталог уже сказал,
# о ком речь, и суффикс `-generator` был бы вторым разом про то же.
.PHONY: lint typecheck test docs inventory

# --- Проверки ---

lint:
	uv run ruff check
	uv run ruff format --check

typecheck:
	uv run ty check

test:
	uv run pytest

# --- Сборка из кода ---

docs:
	uv run python -m clickstream_generator.schema_doc \
		../docs/formats/clickstream-event.md

inventory:
	uv run python -m clickstream_generator.inventory \
		../data/world-inventory.json
