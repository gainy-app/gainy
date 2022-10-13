from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "create-invoices"
tags = ["billing"]
dag = create_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="30 12 1 * *")

operator = BashOperator(task_id="create-invoices",
                        bash_command="gainy_create_invoices",
                        dag=dag)
