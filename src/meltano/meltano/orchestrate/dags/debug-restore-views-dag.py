from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command

dag_id = "debug-restore-views"
tags = ["meltano", "postgres", "dbt"]
dag = create_dag(dag_id, tags=tags)

dbt = BashOperator(task_id="dbt",
                   bash_command=get_meltano_command(
                       "invoke dbt run --model config.materialized:view"),
                   dag=dag)
