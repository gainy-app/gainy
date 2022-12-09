from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "realtime-trading-dag"
tags = ["billing", "trading", "drivewealth"]
dag = create_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="*/5 * * * *")

operator = BashOperator(
    task_id="update-account-balances",
    bash_command="gainy_update_account_balances --realtime",
    dag=dag)
