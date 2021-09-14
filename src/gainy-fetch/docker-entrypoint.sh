#!/bin/sh

( cd scripts && python3 generate_collection_rules_sql.py )

# AIRFLOW
meltano invoke airflow webserver -D -p $AIRFLOW_UI_PORT
meltano invoke airflow scheduler -D

if [ "$1" != "local" ]; then
  meltano ui start --bind-port $MELTANO_UI_PORT
fi