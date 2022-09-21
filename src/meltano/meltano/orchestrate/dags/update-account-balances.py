from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "update-account-balances"
tags = ["billing"]
dag = create_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="15 20 * * *")

operator = BashOperator(
    task_id="update-account-balances",
    bash_command="gainy_update_account_balances",
    dag=dag)
