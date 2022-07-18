meltano invoke airflow pools set dbt 1 dbt
meltano invoke airflow pools set downstream $DOWNSTREAM_POOL_SIZE downstream
meltano invoke airflow pools set gainy_recommendation 1 gainy_recommendation
meltano invoke airflow pools set polygon-to-postgres $POLYGON_TO_POSTGRES_POOL_SIZE polygon-to-postgres
