from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "debug-fetch-drivewealth-instruments"
tags = ["gainy-compute", "trading", "drivewealth"]
dag = create_dag(dag_id, tags=tags)

gainy_fetch_drivewealth_instruments = BashOperator(
    task_id="debug-fetch-drivewealth-instruments",
    bash_command="gainy_fetch_drivewealth_instruments",
    dag=dag)
