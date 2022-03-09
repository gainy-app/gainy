from airflow import DAG
from datetime import datetime, timedelta

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


def create_dag(dag_id: str,
               tags: list,
               default_args={},
               schedule_interval=None,
               max_active_runs=1,
               is_paused_upon_creation=True):

    for k, i in DEFAULT_ARGS.items():
        if k in default_args:
            continue
        default_args[k] = i

    dag = DAG(dag_id,
              tags=tags,
              catchup=False,
              default_args=default_args,
              schedule_interval=schedule_interval,
              max_active_runs=max_active_runs,
              is_paused_upon_creation=is_paused_upon_creation)

    globals()[dag_id] = dag

    return dag
