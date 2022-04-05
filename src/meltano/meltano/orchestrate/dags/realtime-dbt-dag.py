from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command

vars = '{"realtime": true}'
dag_id = "realtime-dbt-dag"
tags = ["meltano", "dbt"]
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval="*/5 * * * *",
                 is_paused_upon_creation=False)

dbt = BashOperator(
    task_id="dbt-realtime",
    bash_command=get_meltano_command(
        f"invoke dbt run --vars '{vars}' --select tag:realtime"),
    dag=dag,
    pool="dbt")
