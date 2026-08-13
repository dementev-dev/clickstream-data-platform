"""Минимальные настройки локального учебного Superset."""

import os

SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET__SQLALCHEMY_DATABASE_URI"]
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]


# Источник данных пока один, поэтому штатному хранилищу паролей Superset
# достаточно вернуть один секрет из окружения.
def clickhouse_password(_url):
    return os.environ["CLICKHOUSE_BI_PASSWORD"]


SQLALCHEMY_CUSTOM_PASSWORD_STORE = clickhouse_password

SESSION_COOKIE_NAME = "superset_session"
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

WTF_CSRF_ENABLED = True
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
