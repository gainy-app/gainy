echo 'Importing seeds'
find seed seed/$ENV -maxdepth 1 -iname '*.sql' | sort | while read -r i; do
  PGPASSWORD=$PG_PASSWORD psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME -P pager -f "$i"
done

PGPASSWORD=$PG_PASSWORD psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME -c \
  "insert into deployment.public_schemas(schema_name) values ('$DBT_TARGET_SCHEMA') on conflict(schema_name) do nothing;"

PGPASSWORD=$PG_PASSWORD psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME -c "CREATE SCHEMA IF NOT EXISTS $DBT_TARGET_SCHEMA;"

if [ $(PGPASSWORD=$PG_PASSWORD psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME -c "select count(*) from deployment.public_schemas where schema_name = '$DBT_TARGET_SCHEMA' and deployed_at is not null" -t --csv) == "0" ]; then
  echo 'Running csv-to-postgres'
  if meltano schedule run csv-to-postgres --force; then
    PGPASSWORD=$PG_PASSWORD psql -h $PG_HOST -p $PG_PORT -U $PG_USERNAME $PG_DBNAME -c \
      "update deployment.public_schemas set deployed_at = now() where schema_name = '$DBT_TARGET_SCHEMA';"
  endif
fi

if [ "$ENV" != "local" ]; then
  nohup bash -c "meltano schedule run postgres-to-search --force" &> /dev/null &
fi

echo "Seeding done"
