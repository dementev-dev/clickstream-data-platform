#!/usr/bin/env bash
set -euo pipefail

# Один сервер, но отдельные владельцы и базы не смешивают метаданные приложений.
psql --set=ON_ERROR_STOP=1 \
    --set=airflow_user="$AIRFLOW_METADATA_USER" \
    --set=airflow_password="$AIRFLOW_METADATA_PASSWORD" \
    --set=superset_user="$SUPERSET_METADATA_USER" \
    --set=superset_password="$SUPERSET_METADATA_PASSWORD" \
    --username "$POSTGRES_USER" \
    --dbname postgres <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'airflow_user') \gexec

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'superset_user', :'superset_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'superset_user') \gexec

SELECT format('CREATE DATABASE airflow OWNER %I', :'airflow_user')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec

SELECT format('CREATE DATABASE superset OWNER %I', :'superset_user')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset') \gexec
SQL
