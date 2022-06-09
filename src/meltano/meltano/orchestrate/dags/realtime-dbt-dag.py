from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, MELTANO_PROJECT_ROOT

dag_id = "realtime-dbt-dag"
tags = ["meltano", "dbt"]
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval="*/5 * * * *" if ENV == "production" else "0 * * * *",
                 is_paused_upon_creation=False)

clean = BashOperator(
    task_id="clean",
    bash_command=
    f"cd {MELTANO_PROJECT_ROOT}; find .meltano/logs .meltano/run/elt -type f -mmin +480 -delete",
    dag=dag)

coingecko_realtime = BashOperator(
    task_id="coingecko-realtime",
    bash_command=get_meltano_command(
        "schedule run coingecko-realtime-to-postgres --force"),
    dag=dag)

vars = '{"realtime": true}'
dbt = BashOperator(
    task_id="dbt-realtime",
    bash_command=get_meltano_command(
        f"invoke dbt run --vars '{vars}' --select tag:realtime"),
    dag=dag,
    pool="dbt")
