export PGPASSWORD="$PG_PASSWORD"
psql_auth="psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME"

$psql_auth -c "create user datadog with password '${PG_DATADOG_PASSWORD}';" || $psql_auth -c "alter user datadog with password '${PG_DATADOG_PASSWORD}';"
$psql_auth -c "grant pg_monitor to datadog;"
$psql_auth -c "grant SELECT ON pg_stat_database to datadog;"
$psql_auth -c "GRANT USAGE ON SCHEMA airflow TO datadog;"
$psql_auth -c "GRANT SELECT ON ALL TABLES IN SCHEMA airflow TO datadog;"

$psql_auth -c "create user ${PG_INTERNAL_SYNC_USERNAME} with password '${PG_INTERNAL_SYNC_PASSWORD}';" || $psql_auth -c "alter user ${PG_INTERNAL_SYNC_USERNAME} with password '${PG_INTERNAL_SYNC_PASSWORD}';"

echo 'Importing seeds'
find seed -maxdepth 1 -iname '*.sql' | sort | while read -r i; do
  $psql_auth -P pager -f "$i"
done

$psql_auth -c "insert into deployment.public_schemas(schema_name) values ('$DBT_TARGET_SCHEMA') on conflict(schema_name) do nothing;"

$psql_auth -c "CREATE SCHEMA IF NOT EXISTS $DBT_TARGET_SCHEMA;"

$psql_auth -c "GRANT USAGE ON SCHEMA raw_data TO ${PG_INTERNAL_SYNC_USERNAME};"
$psql_auth -c "GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO ${PG_INTERNAL_SYNC_USERNAME};"
$psql_auth -c "GRANT USAGE ON SCHEMA $DBT_TARGET_SCHEMA TO datadog;"

export DBT_ARTIFACT_STATE_PATH=/project/.meltano/transformers/dbt/target/
OLD_DBT_TARGET_SCHEMA=$( $psql_auth -c "select schema_name from deployment.public_schemas where deployed_at is not null limit 1" -t --csv )
export DBT_RUN_FLAGS="--full-refresh"

if [ "$OLD_DBT_TARGET_SCHEMA" != "" ]; then
  if [ "$OLD_DBT_TARGET_SCHEMA" != "$DBT_TARGET_SCHEMA" ]; then
    echo "$(date)" Restoring schema "$OLD_DBT_TARGET_SCHEMA" to "$DBT_TARGET_SCHEMA"
    $psql_auth -f scripts/clone_schema.sql
    $psql_auth -c "call clone_schema('$OLD_DBT_TARGET_SCHEMA', '$DBT_TARGET_SCHEMA')"
    echo "$(date)" Schema "$OLD_DBT_TARGET_SCHEMA" restored to  "$DBT_TARGET_SCHEMA"
  fi

  # restore dbt state
  $psql_auth -c "select dbt_state from deployment.public_schemas where schema_name = '$OLD_DBT_TARGET_SCHEMA'" -t --csv > /tmp/dbt_state
  if [ -s /tmp/dbt_state ]; then
    base64 -d /tmp/dbt_state > /tmp/dbt_state.tgz
    tar -xzf /tmp/dbt_state.tgz -C $DBT_ARTIFACT_STATE_PATH
    if [ "$OLD_DBT_TARGET_SCHEMA" != "$DBT_TARGET_SCHEMA" ]; then
      find $DBT_ARTIFACT_STATE_PATH -type f -print0 | xargs -0 sed -i "s/$OLD_DBT_TARGET_SCHEMA/$DBT_TARGET_SCHEMA/g"
    fi
    export DBT_RUN_FLAGS="-s result:error+ state:modified+ source_status:fresher+ --defer"
  fi

  # restore seed data state
  $psql_auth -c "select seed_data_state from deployment.public_schemas where schema_name = '$OLD_DBT_TARGET_SCHEMA'" -t --csv > /tmp/seed_data_state_base64
  if [ -s /tmp/seed_data_state_base64 ]; then
    base64 -d /tmp/seed_data_state_base64 > /tmp/seed_data_state.gz
    gunzip -f /tmp/seed_data_state.gz
  fi
fi

if [ ! -f /tmp/seed_data_state ] || ! md5sum -c /tmp/seed_data_state; then
  echo "$(date)" 'Running csv-to-postgres'
  meltano schedule run csv-to-postgres --force --transform skip
  find seed/$ENV -maxdepth 1 -iname '*.sql' | sort | while read -r i; do
    $psql_auth -P pager -f "$i"
  done
fi

echo "$(date)" meltano invoke dbt source freshness
meltano invoke dbt source freshness

echo "$(date)" meltano invoke dbt run $DBT_RUN_FLAGS
if meltano invoke dbt run $DBT_RUN_FLAGS; then
  $psql_auth -c "update deployment.public_schemas set deployed_at = now() where schema_name = '$DBT_TARGET_SCHEMA';"

  scripts/store_deployment_state.sh
else
  echo 'Failed to seed public schema, exiting'
  exit 1
fi

if [ "$ENV" != "local" ]; then
  nohup bash -c "meltano schedule run postgres-to-search --force" &> /dev/null &
fi

echo "Seeding done"
