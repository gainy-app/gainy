import os
import logging
import subprocess
import json

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

# Schedules
result = subprocess.run(
    [meltano_bin, "schedule", "list", "--format=json"],
    cwd=project_root,
    stdout=subprocess.PIPE,
    universal_newlines=True,
    check=True,
)
schedules = json.loads(result.stdout)

# Tags
tags = ["meltano"]
for schedule in schedules:
    if not schedule['extractor'] in tags:
        tags.append(schedule['extractor'])
    if not schedule['loader'] in tags:
        tags.append(schedule['loader'])
    if (schedule['loader'] == 'run' or 'only') and not "dbt" in tags:
        tags.append('dbt')

# DAG
dag_id = "gainy-dag"
dag = DAG(
    dag_id,
    tags=tags,
    catchup=False,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 21 * * 1-5",
    max_active_runs=1,
    is_paused_upon_creation=True
)

# operators
loaders = []
for schedule in schedules:
    logger.info(f"Considering schedule '{schedule['name']}")
    loader = BashOperator(
        task_id=f"{schedule['name']}",
        bash_command=f"cd {project_root}; {meltano_bin} schedule run {schedule['name']} --transform=skip",
        dag=dag,
        task_concurrency=concurrency,
        pool_slots=concurrency
    )
    loaders.append(loader)

dbt = BashOperator(
    task_id="dbt-transform",
    bash_command=f"cd {project_root}; {meltano_bin} invoke dbt run",
    dag=dag,
    pool="dbt"
)

# dependencies
loaders >> dbt

# register the dag
globals()[dag_id] = dag

logger.info(f"DAG '{dag_id}' created")
