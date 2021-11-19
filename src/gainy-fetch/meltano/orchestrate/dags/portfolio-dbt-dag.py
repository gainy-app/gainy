import os
import logging

from airflow import DAG
try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

project_root = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())
concurrency = int(os.getenv("EODHISTORICALDATA_JOBS_COUNT", 1))

ENV = os.getenv("ENV")
DEFAULT_ARGS = {
    "owner": "gainy",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "concurrency": concurrency,
    "start_date": datetime(2021, 9, 1)
}


# DAG

##################################################################################################################

dag_id = "portfolio-dbt-dag"
tags = ["meltano", "dbt"]
dag = DAG(
    dag_id,
    tags=tags,
    catchup=False,
    default_args=DEFAULT_ARGS,
    schedule_interval="* * * * 1-5" if ENV == "production" else "*/5 * * * 1-5",
    max_active_runs=1,
    is_paused_upon_creation=False
)
dbt = BashOperator(
    task_id="dbt-portfolio",
    bash_command=f"cd {project_root}; {meltano_bin} invoke dbt run --model portfolio_holding_gains portfolio_transaction_gains portfolio_gains",
    dag=dag,
    pool="dbt"
)

# register the dag
globals()[dag_id] = dag

logger.info(f"DAG '{dag_id}' created")
