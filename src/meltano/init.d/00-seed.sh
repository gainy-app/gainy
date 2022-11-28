export PGPASSWORD="$PG_PASSWORD"
psql_auth="psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME"

#$psql_auth -c "REVOKE SELECT ON ALL TABLES IN SCHEMA airflow FROM datadog; REVOKE USAGE ON SCHEMA airflow FROM datadog; drop user if exists datadog" 2> /dev/null
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

if [ $($psql_auth -c "select count(*) from deployment.public_schemas where schema_name = '$DBT_TARGET_SCHEMA' and deployed_at is not null" -t --csv) == "0" ]; then
  echo 'Running csv-to-postgres'
  meltano schedule run csv-to-postgres --force --transform skip
  find seed/$ENV -maxdepth 1 -iname '*.sql' | sort | while read -r i; do
    $psql_auth -P pager -f "$i"
  done
  if meltano invoke dbt run --full-refresh; then
    $psql_auth -c "update deployment.public_schemas set deployed_at = now() where schema_name = '$DBT_TARGET_SCHEMA';"
  else
    echo 'Failed to seed public schema, exiting'
    exit 1
  fi
fi

if [ "$ENV" != "local" ]; then
  nohup bash -c "meltano schedule run postgres-to-search --force" &> /dev/null &
fi

echo "Seeding done"
