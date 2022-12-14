#!/bin/bash

export PGPASSWORD="$PG_PASSWORD"
psql_auth="psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME"

# store dbt_state
tar -cvzf /tmp/dbt_state.tgz -C "$DBT_ARTIFACT_STATE_PATH" .
$psql_auth <<< "update deployment.public_schemas set dbt_state = '$(base64 -w 0 /tmp/dbt_state.tgz)' where schema_name = '$DBT_TARGET_SCHEMA';"

# store seed_data_state
find data -type f -exec md5sum "{}" + | gzip > /tmp/seed_data_state.gz
$psql_auth <<< "update deployment.public_schemas set seed_data_state = '$(base64 -w 0 /tmp/seed_data_state.gz)' where schema_name = '$DBT_TARGET_SCHEMA';"
