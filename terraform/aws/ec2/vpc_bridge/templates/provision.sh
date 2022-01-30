#!/bin/bash

DD_AGENT_MAJOR_VERSION=7 DD_API_KEY="${datadog_api_key}" DD_SITE="datadoghq.com" bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"
apt install -y postgresql-client
PGPASSWORD="${pg_password}" psql -h "${pg_host}" -p "${pg_port}" -U "${pg_username}" "${pg_dbname}" -P pager -c "REVOKE SELECT ON ALL TABLES IN SCHEMA airflow FROM datadog; REVOKE USAGE ON SCHEMA airflow FROM datadog; drop user if exists datadog" 2> /dev/null
PGPASSWORD="${pg_password}" psql -h "${pg_host}" -p "${pg_port}" -U "${pg_username}" "${pg_dbname}" -P pager -c "create user datadog with password '${pg_datadog_password}'; grant pg_monitor to datadog; grant SELECT ON pg_stat_database to datadog; GRANT USAGE ON SCHEMA airflow TO datadog;GRANT SELECT ON ALL TABLES IN SCHEMA airflow TO datadog;"

PGPASSWORD="${pg_password}" psql -h "${pg_host}" -p "${pg_port}" -U "${pg_username}" "${pg_dbname}" -P pager -c "REVOKE SELECT ON ALL TABLES IN SCHEMA raw_data FROM ${pg_internal_sync_username}; REVOKE USAGE ON SCHEMA raw_data FROM ${pg_internal_sync_username}; drop user if exists ${pg_internal_sync_username}" 2> /dev/null
PGPASSWORD="${pg_password}" psql -h "${pg_host}" -p "${pg_port}" -U "${pg_username}" "${pg_dbname}" -P pager -c "create user ${pg_internal_sync_username} with password '${pg_internal_sync_password}'; GRANT USAGE ON SCHEMA raw_data TO ${pg_internal_sync_username}; GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO ${pg_internal_sync_username};"

PGPASSWORD="${pg_password}" psql -h "${pg_host}" -p "${pg_port}" -U "${pg_username}" "${pg_dbname}" -P pager -c "create extension if not exists pg_stat_statements;"

cp /tmp/datadog.postgres.yaml /etc/datadog-agent/conf.d/postgres.d/conf.yaml
service datadog-agent restart
