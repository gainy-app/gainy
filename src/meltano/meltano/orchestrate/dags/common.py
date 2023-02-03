import os
import subprocess
import json
import logging
from pathlib import Path
from airflow import DAG
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
MELTANO_PROJECT_ROOT = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())
ENV = os.getenv("ENV")


def create_dag(dag_id: str,
               tags: list,
               default_args={},
               schedule_interval=None,
               max_active_runs=1,
               is_paused_upon_creation=True) -> DAG:

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


def get_meltano_bin() -> str:
    meltano_bin = ".meltano/run/bin"
    if not Path(MELTANO_PROJECT_ROOT).joinpath(meltano_bin).exists():
        logger.warning(
            f"A symlink to the 'meltano' executable could not be found at '{meltano_bin}'. Falling back on expecting it to be in the PATH instead."
        )
        return "meltano"

    return meltano_bin


def get_meltano_command(meltano_cmd: str) -> str:
    return f"cd {MELTANO_PROJECT_ROOT}; {get_meltano_bin()} {meltano_cmd}"


def get_schedules(include_skipped=False):
    result = subprocess.run(
        [get_meltano_bin(), "schedule", "list", "--format=json"],
        cwd=MELTANO_PROJECT_ROOT,
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
    )
    schedules = json.loads(result.stdout)['schedules']['elt']

    # Process schedule parameters
    for schedule in schedules:
        env = schedule.get('env', {})

        if 'TARGET_ENVS' in env:
            target_envs = json.loads(env['TARGET_ENVS'])
            schedule['skipped'] = ENV not in target_envs
        else:
            schedule['skipped'] = False

        schedule['downstream'] = "DOWNSTREAM" == env.get(
            'INTEGRATION', 'UPSTREAM')

    if include_skipped:
        return schedules

    return list(filter(lambda schedule: not schedule['skipped'], schedules))
