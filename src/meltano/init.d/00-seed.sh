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

if [ $($psql_auth -c "select count(*) from deployment.public_schemas where schema_name = '$DBT_TARGET_SCHEMA' and deployed_at is not null" -t --csv) == "0" ]; then
  export DBT_RUN_FLAGS="--full-refresh"

  OLD_DBT_TARGET_SCHEMA=$( $psql_auth -c "select schema_name from deployment.public_schemas where deleted_at is null and deployed_at is not null order by deployed_at desc limit 1" -t --csv )
  echo OLD_DBT_TARGET_SCHEMA "$OLD_DBT_TARGET_SCHEMA"

  if [ "$OLD_DBT_TARGET_SCHEMA" != "" ]; then
    $psql_auth -c "select seed_data_state from deployment.public_schemas where schema_name = '$OLD_DBT_TARGET_SCHEMA' and seed_data_state is not null" -t --csv > /tmp/seed_data_state_base64
    if [ -s /tmp/seed_data_state_base64 ]; then
      echo "$(date)" Restoring seed data state
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
  elif [ "$OLD_DBT_TARGET_SCHEMA" != "" ]; then
    $psql_auth -c "select dbt_state from deployment.public_schemas where schema_name = '$OLD_DBT_TARGET_SCHEMA' and dbt_state is not null" -t --csv > /tmp/dbt_state
    if [ -s /tmp/dbt_state ]; then
      echo "$(date)" Restoring dbt state
      base64 -d /tmp/dbt_state > /tmp/dbt_state.tgz
      mkdir -p "$DBT_ARTIFACT_STATE_PATH"
      tar -xzf /tmp/dbt_state.tgz -C "$DBT_ARTIFACT_STATE_PATH"

      find $DBT_ARTIFACT_STATE_PATH -type f -print0 | xargs -0 sed -i "s/$OLD_DBT_TARGET_SCHEMA/$DBT_TARGET_SCHEMA/g"

      echo "$(date)" Restoring schema "$OLD_DBT_TARGET_SCHEMA" to "$DBT_TARGET_SCHEMA"
      $psql_auth -f scripts/clone_schema.sql
      $psql_auth -c "DROP SCHEMA IF EXISTS $DBT_TARGET_SCHEMA;"
      if $psql_auth -c "call clone_schema('$OLD_DBT_TARGET_SCHEMA', '$DBT_TARGET_SCHEMA')"; then
        echo "$(date)" Schema "$OLD_DBT_TARGET_SCHEMA" restored to "$DBT_TARGET_SCHEMA"
        export DBT_RUN_FLAGS="-s result:error+ state:modified+ config.materialized:view --defer --full-refresh"
      else
        echo "$(date)" Failed to restore schema "$OLD_DBT_TARGET_SCHEMA" to "$DBT_TARGET_SCHEMA, doing full refresh"
      fi
    fi
  fi

  echo "$(date)" meltano invoke dbt run $DBT_RUN_FLAGS
  if meltano invoke dbt run $DBT_RUN_FLAGS; then
    $psql_auth -c "update deployment.public_schemas set deployed_at = now() where schema_name = '$DBT_TARGET_SCHEMA';"

    /bin/bash scripts/store_deployment_state.sh
  else
    echo 'Failed to seed public schema, exiting'
    exit 1
  fi
fi

$psql_auth -c "GRANT USAGE ON SCHEMA raw_data TO ${PG_INTERNAL_SYNC_USERNAME};"
$psql_auth -c "GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO ${PG_INTERNAL_SYNC_USERNAME};"
$psql_auth -c "GRANT USAGE ON SCHEMA $DBT_TARGET_SCHEMA TO datadog;"

if [ "$ENV" != "local" ]; then
  nohup bash -c "meltano schedule run postgres-to-search --force" &> /dev/null &
fi

echo "Seeding done"
