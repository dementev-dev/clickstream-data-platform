"""Общее для всех проверок: чистое окружение прогона.

Переменные запуска живут на этой машине по-настоящему — стенд их экспортирует.
Тест, читающий их у машины, зелен у одного и красен у другого, поэтому интерфейс
запуска проверяется только тем, что ему передали аргументами.
"""

import pytest

LAUNCH_VARIABLES = (
    "GENERATOR_SEED",
    "GENERATOR_DAY",
    "GENERATOR_DAYS",
    "GENERATOR_LIMIT",
    "GENERATOR_SPEED",
    "GENERATOR_FILE",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
)


@pytest.fixture(autouse=True)
def bare_environment(monkeypatch):
    """Прогон тестов не зависит от того, что задано в окружении машины."""
    for name in LAUNCH_VARIABLES:
        monkeypatch.delenv(name, raising=False)
