import logging

from airflow import DAG
try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "gainy",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 12, 1)
}

dag_id = "industry-assignments-train"
tags = ["gainy-compute", "mlflow", "train"]
dag = DAG(dag_id,
          tags=tags,
          catchup=False,
          default_args=DEFAULT_ARGS,
          schedule_interval=None,
          max_active_runs=1,
          is_paused_upon_creation=True)

industry_assignments_train = BashOperator(
    task_id="industry-assignments-train",
    bash_command="gainy_industry_assignment train",
    dag=dag)

# register the dag
globals()[dag_id] = dag

logger.info(f"DAG '{dag_id}' created")
