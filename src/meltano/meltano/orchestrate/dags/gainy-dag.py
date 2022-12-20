from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, get_schedules, MELTANO_PROJECT_ROOT, ENV

schedules = get_schedules()
tags = {
    schedule['loader'] if schedule['downstream'] else schedule['extractor']
    for schedule in schedules
}

# DAG
dag_id = "gainy-dag"
dag = create_dag(
    dag_id,
    tags=list(tags),
    schedule_interval="0 4 * * *" if ENV == "production" else "0 6 * * *",
    is_paused_upon_creation=True)

# Operators
upstream = []
downstream = []

for schedule in schedules:
    pool = None
    if not schedule['downstream']:
        pool = "upstream"

    operator = BashOperator(
        task_id=schedule['name'],
        bash_command=get_meltano_command(
            f"schedule run {schedule['name']} --transform=skip --force"),
        skip_exit_code=1,
        dag=dag,
        pool=pool)

    if schedule['downstream']:
        downstream.append(operator)
    else:
        upstream.append(operator)

dbt = BashOperator(task_id="dbt",
                   bash_command=get_meltano_command(
                       "invoke dbt run --exclude config.materialized:view"),
                   dag=dag,
                   trigger_rule="all_done",
                   pool="dbt")

clean = BashOperator(task_id="clean",
                     cwd=MELTANO_PROJECT_ROOT,
                     bash_command="/venv/bin/python scripts/cleanup.py",
                     dag=dag)

generate_meltano_config = BashOperator(
    task_id="generate_meltano_config",
    cwd=MELTANO_PROJECT_ROOT,
    bash_command="/venv/bin/python scripts/generate_meltano_config.py",
    dag=dag)

store_deployment_state = BashOperator(
    task_id="store_deployment_state",
    cwd=MELTANO_PROJECT_ROOT,
    bash_command='{{"/bin/bash scripts/store_deployment_state.sh"}}',
    dag=dag)

generate_meltano_config >> upstream >> dbt >> downstream >> clean
dbt >> store_deployment_state
