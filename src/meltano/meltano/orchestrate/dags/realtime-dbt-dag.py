from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, MELTANO_PROJECT_ROOT, ENV

dag_id = "realtime-dbt-dag"
tags = ["meltano", "dbt"]
schedule_interval = "*/15 * * * *" if ENV == "production" else "*/15 * * * *"
is_paused_upon_creation = ENV != "production"
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval=schedule_interval,
                 is_paused_upon_creation=is_paused_upon_creation)

clean = BashOperator(
    task_id="clean",
    bash_command=
    f"cd {MELTANO_PROJECT_ROOT}; find .meltano/logs .meltano/run/elt -type f -mmin +480 -delete",
    dag=dag)

vars = '{"realtime": true}'
dbt = BashOperator(
    task_id="dbt-realtime",
    bash_command=get_meltano_command(
        f"invoke dbt run --vars '{vars}' --select tag:realtime"),
    dag=dag,
    pool="dbt")
