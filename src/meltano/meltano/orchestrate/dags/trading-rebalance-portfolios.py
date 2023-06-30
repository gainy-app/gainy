import os

from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "trading-rebalance-portfolios"
tags = ["trading", "drivewealth"]
dag = create_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="*/5 12-21 * * *")

batch_cnt = int(os.getenv("TRADING_REBALANCE_JOBS_COUNT", "1"))
for batch_id in range(batch_cnt):
    BashOperator(
        task_id=f"rebalance-portfolios-{batch_id}",
        bash_command=
        f"gainy_rebalance_portfolios --batch-id {batch_id} --batch-cnt {batch_cnt} 2>&1 | tee /proc/1/fd/1",
        dag=dag)
