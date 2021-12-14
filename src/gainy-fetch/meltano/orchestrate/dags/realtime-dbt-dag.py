import os
import logging

from airflow import DAG
try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
from pathlib import Path


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


# Meltano
meltano_bin = ".meltano/run/bin"
if not Path(project_root).joinpath(meltano_bin).exists():
    logger.warning(
        f"A symlink to the 'meltano' executable could not be found at '{meltano_bin}'. Falling back on expecting it to be in the PATH instead."
    )
    meltano_bin = "meltano"

# DAG

##################################################################################################################

models = [
    'historical_prices_aggregated',
    'portfolio_chart',
    'portfolio_expanded_transactions',
    'portfolio_gains',
    'portfolio_holding_details',
    'portfolio_holding_gains',
    'portfolio_holding_group_details',
    'portfolio_holding_group_gains',
    'portfolio_securities_normalized',
    'portfolio_transaction_gains',
    'profile_holding_groups',
    'profile_holdings_normalized',
    'ticker_realtime_metrics',
]

dag_id = "realtime-dbt-dag"
tags = ["meltano", "dbt"]
dag = DAG(
    dag_id,
    tags=tags,
    catchup=False,
    default_args=DEFAULT_ARGS,
    schedule_interval="1,16,31,46 * * * 1-5",
    max_active_runs=1,
    is_paused_upon_creation=False
)
dbt = BashOperator(
    task_id="dbt-portfolio",
    bash_command=f"cd {project_root}; {meltano_bin} invoke dbt run --model " + " ".join(models),
    dag=dag,
    pool="dbt"
)

# register the dag
globals()[dag_id] = dag

logger.info(f"DAG '{dag_id}' created")
