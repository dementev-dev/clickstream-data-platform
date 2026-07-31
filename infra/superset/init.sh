#!/usr/bin/env bash
set -euo pipefail

superset db upgrade
if python - <<'PY'
import os

from sqlalchemy import create_engine, text

engine = create_engine(os.environ["SUPERSET__SQLALCHEMY_DATABASE_URI"])
with engine.connect() as connection:
    exists = connection.scalar(
        text("SELECT EXISTS (SELECT 1 FROM ab_user WHERE username = :username)"),
        {"username": os.environ["SUPERSET_ADMIN_USER"]},
    )
raise SystemExit(0 if exists else 1)
PY
then
    superset fab reset-password \
        --username "$SUPERSET_ADMIN_USER" \
        --password "$SUPERSET_ADMIN_PASSWORD"
else
    superset fab create-admin \
        --username "$SUPERSET_ADMIN_USER" \
        --password "$SUPERSET_ADMIN_PASSWORD" \
        --firstname Superset \
        --lastname Admin \
        --email admin@example.invalid
fi
superset init
superset import-directory /app/import --overwrite
