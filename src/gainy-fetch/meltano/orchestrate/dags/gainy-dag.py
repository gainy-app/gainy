import os
from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, get_schedules, MELTANO_PROJECT_ROOT, ENV

CONCURRENCY = int(os.getenv("EODHISTORICALDATA_JOBS_COUNT", 1))


def create_downstream_operators(dag):
    industry_assignment = BashOperator(
        task_id="industry-assignments-generator",
        bash_command="gainy_industry_assignment predict",
        dag=dag)
    recommendation = BashOperator(task_id="update-recommendations",
                                  bash_command="gainy_recommendation",
                                  dag=dag)
    industry_assignment >> recommendation
    return [industry_assignment]


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
    schedule_interval="0 23 * * *" if ENV == "production" else "0 0 * * 1-5",
    is_paused_upon_creation=False)

# Operators
upstream = []
downstream = []

for schedule in schedules:
    operator = BashOperator(
        task_id=schedule['name'],
        bash_command=get_meltano_command(
            f"schedule run {schedule['name']} --transform=skip"),
        dag=dag,
        task_concurrency=CONCURRENCY,
        pool_slots=CONCURRENCY)

    if schedule['downstream']:
        downstream.append(operator)
    else:
        upstream.append(operator)

dbt = BashOperator(task_id="dbt",
                   bash_command=get_meltano_command("invoke dbt run"),
                   dag=dag,
                   pool="dbt")

clean = BashOperator(
    task_id="clean",
    bash_command=f"cd {MELTANO_PROJECT_ROOT}; python3 scripts/cleanup.py",
    dag=dag)

downstream += create_downstream_operators(dag)

# dependencies
upstream >> dbt >> downstream
