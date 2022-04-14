if [ "$ENV" == "local" ]; then
  meltano invoke dbt docs generate
fi
