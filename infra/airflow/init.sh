#!/usr/bin/env bash
set -euo pipefail

install -d -o airflow -g root -m 0775 /opt/airflow/auth

# SimpleAuthManager не умеет создавать пользователей через CLI: пароль хранится в JSON.
runuser -u airflow -- python - <<'PY'
import json
import os
from pathlib import Path

passwords_file = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
passwords_file.write_text(
    json.dumps(
        {os.environ["AIRFLOW_ADMIN_USER"]: os.environ["AIRFLOW_ADMIN_PASSWORD"]},
        ensure_ascii=False,
    )
    + "\n",
    encoding="utf-8",
)
PY

runuser -u airflow -- airflow db migrate
runuser -u airflow -- airflow connections delete clickhouse_default >/dev/null 2>&1 || true
runuser -u airflow -- airflow connections add clickhouse_default \
    --conn-type generic \
    --conn-host clickhouse-01 \
    --conn-port 8123 \
    --conn-login etl \
    --conn-password "$CLICKHOUSE_ETL_PASSWORD" \
    --conn-schema default \
    --conn-description "ClickHouse, нода 1; типизированный провайдер появится на этапе ETL"
