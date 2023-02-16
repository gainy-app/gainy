from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, ENV, get_schedules, MELTANO_PROJECT_ROOT

dag_id = "realtime-polygon-dag"
tags = ["meltano"]
schedule_interval = "*/5 * * * *"
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval=schedule_interval,
                 is_paused_upon_creation=False)

generate_meltano_config = BashOperator(
    task_id="generate_meltano_config",
    bash_command=
    f"cd {MELTANO_PROJECT_ROOT}; /venv/bin/python scripts/generate_meltano_config.py",
    dag=dag)

schedules = get_schedules(include_skipped=True)
for schedule in schedules:
    if not schedule['name'].startswith("polygon-intraday-to-postgres"):
        continue

    operator = BashOperator(
        task_id=schedule['name'],
        pool="polygon",
        bash_command=get_meltano_command(
            f"schedule run {schedule['name']} --transform=skip --force"),
        dag=dag)

    generate_meltano_config >> operator
