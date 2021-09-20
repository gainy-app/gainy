#!/bin/sh

python scripts/generate_meltano_config.py $ENV

PGPASSWORD=$PG_PASSWORD psql -h $PG_ADDRESS -p $PG_PORT -U $PG_USERNAME $PG_DATABASE -c "CREATE SCHEMA IF NOT EXISTS meltano"
PGPASSWORD=$PG_PASSWORD psql -h $PG_ADDRESS -p $PG_PORT -U $PG_USERNAME $PG_DATABASE -c "CREATE SCHEMA IF NOT EXISTS airflow"
if ! meltano invoke airflow users list | grep admin > /dev/null; then
  meltano invoke airflow users create --username admin --password $AIRFLOW_PASSWORD --firstname admin --lastname admin --role Admin --email support@gainy.app
fi

( cd scripts && python3 generate_collection_rules_sql.py )

supervisord -n -c /etc/supervisor/supervisord.conf