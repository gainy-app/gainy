from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, ENV, get_schedules

dag_id = "realtime-polygon-dag"
tags = ["meltano"]
schedule_interval = "*/15 * * * *" if ENV == "production" else "*/60 * * * *"
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval=schedule_interval,
                 is_paused_upon_creation=False)

schedules = get_schedules(include_skipped=True)
for schedule in schedules:
    if not schedule['name'].startswith("polygon-intraday-to-postgres"):
        continue

    operator = BashOperator(
        task_id=schedule['name'],
        bash_command=get_meltano_command(
            f"schedule run {schedule['name']} --transform=skip"),
        dag=dag)
