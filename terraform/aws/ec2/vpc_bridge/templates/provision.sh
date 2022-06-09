#!/bin/bash

DD_AGENT_MAJOR_VERSION=7 DD_API_KEY="${DATADOG_API_KEY}" DD_SITE="datadoghq.com" bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"
apt install -y postgresql-client
PGPASSWORD="${PG_PASSWORD}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USERNAME}" "${PG_DBNAME}" -P pager -c "REVOKE SELECT ON ALL TABLES IN SCHEMA airflow FROM datadog; REVOKE USAGE ON SCHEMA airflow FROM datadog; drop user if exists datadog" 2> /dev/null
PGPASSWORD="${PG_PASSWORD}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USERNAME}" "${PG_DBNAME}" -P pager -c "create user datadog with password '${PG_DATADOG_PASSWORD}'; grant pg_monitor to datadog; grant SELECT ON pg_stat_database to datadog; GRANT USAGE ON SCHEMA airflow TO datadog;GRANT SELECT ON ALL TABLES IN SCHEMA airflow TO datadog;"

PGPASSWORD="${PG_PASSWORD}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USERNAME}" "${PG_DBNAME}" -P pager -c "CREATE SCHEMA IF NOT EXISTS ${PUBLIC_SCHEMA_NAME};"
PGPASSWORD="${PG_PASSWORD}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USERNAME}" "${PG_DBNAME}" -P pager -c "GRANT USAGE ON SCHEMA ${PUBLIC_SCHEMA_NAME} TO datadog; GRANT SELECT ON ALL TABLES IN SCHEMA ${PUBLIC_SCHEMA_NAME} TO datadog;"

PGPASSWORD="${PG_PASSWORD}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USERNAME}" "${PG_DBNAME}" -P pager -c "REVOKE SELECT ON ALL TABLES IN SCHEMA raw_data FROM ${PG_INTERNAL_SYNC_USERNAME}; REVOKE USAGE ON SCHEMA raw_data FROM ${PG_INTERNAL_SYNC_USERNAME}; drop user if exists ${PG_INTERNAL_SYNC_USERNAME}" 2> /dev/null
PGPASSWORD="${PG_PASSWORD}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USERNAME}" "${PG_DBNAME}" -P pager -c "create user ${PG_INTERNAL_SYNC_USERNAME} with password '${PG_INTERNAL_SYNC_PASSWORD}'; GRANT USAGE ON SCHEMA raw_data TO ${PG_INTERNAL_SYNC_USERNAME}; GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO ${PG_INTERNAL_SYNC_USERNAME};"

PGPASSWORD="${PG_PASSWORD}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USERNAME}" "${PG_DBNAME}" -P pager -c "create extension if not exists pg_stat_statements;"

cp /tmp/datadog.postgres.yaml /etc/datadog-agent/conf.d/postgres.d/conf.yaml
service datadog-agent restart

# delete self
shred -u provision.sh
