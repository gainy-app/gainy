import os
from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, get_schedules, MELTANO_PROJECT_ROOT, ENV


def create_downstream_operators(dag):
    industry_assignment = BashOperator(
        task_id="industry-assignments-generator",
        bash_command="gainy_industry_assignment predict",
        dag=dag)

    recommendation = BashOperator(task_id="update-recommendations",
                                  bash_command="gainy_recommendation",
                                  dag=dag,
                                  pool="gainy_recommendation")

    return industry_assignment, recommendation


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
    schedule_interval="0 1 * * *" if ENV == "production" else "0 2 * * 1-5",
    is_paused_upon_creation=True)

# Operators
upstream = []
downstream = []

for schedule in schedules:
    pool = None
    if not schedule['downstream']:
        pool = "downstream"

    operator = BashOperator(
        task_id=schedule['name'],
        bash_command=get_meltano_command(
            f"schedule run {schedule['name']} --transform=skip"),
        skip_exit_code=1,
        dag=dag,
        pool=pool)

    if schedule['downstream']:
        downstream.append(operator)
    else:
        upstream.append(operator)

dbt = BashOperator(
    task_id="dbt",
    bash_command=get_meltano_command("invoke dbt run --exclude tag:view"),
    dag=dag,
    pool="dbt")

clean = BashOperator(
    task_id="clean",
    bash_command=
    f"cd {MELTANO_PROJECT_ROOT}; /usr/local/bin/python scripts/cleanup.py",
    dag=dag)

industry_assignment, _downstream = create_downstream_operators(dag)
downstream.append(_downstream)

# dependencies
upstream >> industry_assignment >> dbt >> downstream >> clean
