from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command

dag_id = "deployment"
tags = ["meltano", "csv", "postgres", "dbt"]
dag = create_dag(dag_id, tags=tags, is_paused_upon_creation=False)

dbt = BashOperator(task_id="dbt",
                   bash_command=get_meltano_command(
                       "schedule run csv-to-postgres --force --transform run"),
                   dag=dag,
                   pool="dbt")
