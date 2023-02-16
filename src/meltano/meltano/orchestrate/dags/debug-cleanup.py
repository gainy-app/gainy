from airflow.operators.bash import BashOperator
from common import create_dag, MELTANO_PROJECT_ROOT

dag_id = "debug-cleanup"
tags = ["debug"]
dag = create_dag(dag_id, tags=tags)

clean = BashOperator(task_id="clean",
                     cwd=MELTANO_PROJECT_ROOT,
                     bash_command="/venv/bin/python scripts/cleanup.py",
                     dag=dag)