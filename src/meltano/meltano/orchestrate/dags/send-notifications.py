from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "send-notifications"
tags = ["notifications"]
dag = create_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="* * * * *")

operator = BashOperator(task_id="send-notifications",
                        bash_command="send_notifications.py",
                        dag=dag)
