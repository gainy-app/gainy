from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "debug-fetch-drivewealth-countries"
tags = ["gainy-compute", "trading", "drivewealth"]
dag = create_dag(dag_id, tags=tags)

gainy_fetch_drivewealth_countries = BashOperator(
    task_id="debug-fetch-drivewealth-countries",
    bash_command="gainy_fetch_drivewealth_countries",
    dag=dag)
