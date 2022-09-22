from airflow.operators.bash import BashOperator
from common import charge_dag

dag_id = "charge-invoices"
tags = ["billing"]
dag = charge_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="45 12 * * *")

operator = BashOperator(task_id="charge-invoices",
                        bash_command="gainy_charge_invoices",
                        dag=dag)
