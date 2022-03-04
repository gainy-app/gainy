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

# Schedules
result = subprocess.run(
    [meltano_bin, "schedule", "list", "--format=json"],
    cwd=project_root,
    stdout=subprocess.PIPE,
    universal_newlines=True,
    check=True,
)
schedules = json.loads(result.stdout)

# Process schedule parameters
for schedule in schedules:
    env = schedule.get('env', {})

    if 'TARGET_ENVS' in env:
        target_envs = json.loads(env['TARGET_ENVS'])
        schedule['skipped'] = ENV not in target_envs
    else:
        schedule['skipped'] = False

    schedule['downstream'] = "DOWNSTREAM" == env.get('INTEGRATION', 'UPSTREAM')

# Tags
tags = {'dbt'}
for schedule in schedules:
    if schedule['skipped']:
        continue

    if schedule['downstream']:
        tags.add(schedule['loader'])
    else:
        tags.add(schedule['extractor'])

# DAG
dag_id = "gainy-dag"
dag = DAG(
    dag_id,
    tags=list(tags),
    catchup=False,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 23 * * *" if ENV == "production" else "0 0 * * 1-5",
    max_active_runs=1,
    is_paused_upon_creation=True)

# Operators
upstream = []
downstream = []

for schedule in schedules:
    if schedule['skipped']:
        continue

    logger.info(f"Considering schedule '{schedule['name']}")
    operator = BashOperator(
        task_id=f"{schedule['name']}",
        bash_command=
        f"cd {project_root}; {meltano_bin} schedule run {schedule['name']} --transform=skip",
        dag=dag,
        task_concurrency=concurrency,
        pool_slots=concurrency)

    if schedule['downstream']:
        downstream.append(operator)
    else:
        upstream.append(operator)

dbt = BashOperator(
    task_id="dbt",
    bash_command=f"cd {project_root}; {meltano_bin} invoke dbt run",
    dag=dag,
    pool="dbt")

clean = BashOperator(
    task_id="clean",
    bash_command=f"cd {project_root}; python3 scripts/cleanup.py",
    dag=dag)

recommendation = BashOperator(task_id="update-recommendations",
                              bash_command="gainy_recommendation",
                              dag=dag)

# dependencies
upstream >> dbt >> downstream >> recommendation

# register the dag
globals()[dag_id] = dag

logger.info(f"DAG '{dag_id}' created")
