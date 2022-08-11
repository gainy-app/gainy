#!/bin/bash

DD_AGENT_MAJOR_VERSION=7 DD_API_KEY="${DATADOG_API_KEY}" DD_SITE="datadoghq.com" bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"
apt install -y postgresql-client

PGPASSWORD="${PG_PASSWORD}" psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USERNAME}" "${PG_DBNAME}" -P pager -c "create extension if not exists pg_stat_statements;"

cp /tmp/datadog.postgres.yaml /etc/datadog-agent/conf.d/postgres.d/conf.yaml
service datadog-agent restart

# delete self
SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )/$(basename $0)"
shred -u $SCRIPTPATH
