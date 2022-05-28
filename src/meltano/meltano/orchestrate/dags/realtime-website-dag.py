import os
import sys
from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command

if os.getenv("ENV") != "production":
    sys.exit(0)

dag_id = "realtime-website-dag"
tags = ["meltano", "tap-polygon", "target-postgres"]
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval="*/5 * * * *",
                 is_paused_upon_creation=False)

dbt = BashOperator(task_id="website-sync",
                   bash_command=get_meltano_command(
                       "schedule run website-to-postgres --force"),
                   dag=dag)
