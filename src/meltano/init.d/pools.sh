meltano invoke airflow pools set dbt 1 dbt
meltano invoke airflow pools set upstream $UPSTREAM_POOL_SIZE upstream
meltano invoke airflow pools set downstream $DOWNSTREAM_POOL_SIZE downstream
meltano invoke airflow pools set gainy_recommendation 1 gainy_recommendation
meltano invoke airflow pools set polygon $POLYGON_POOL_SIZE polygon
