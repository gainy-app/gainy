from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "trading-rebalance-portfolios"
tags = ["trading", "drivewealth"]
dag = create_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="*/5 12-21 * * Mon-Fri")

operator = BashOperator(
    task_id="rebalance-portfolios",
    bash_command="gainy_rebalance_portfolios 2>&1 | tee /proc/1/fd/1",
    dag=dag)
