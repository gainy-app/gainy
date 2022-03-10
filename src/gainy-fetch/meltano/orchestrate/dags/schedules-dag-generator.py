from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, get_schedules

DEFAULT_TAGS = ["meltano", "debug"]

schedules = get_schedules()

for schedule in schedules:
    dag_id = f"debug-{schedule['name']}"

    tags = DEFAULT_TAGS.copy()
    if schedule["extractor"]:
        tags.append(schedule["extractor"])
    if schedule["loader"]:
        tags.append(schedule["loader"])
    if schedule["transform"] == "run":
        tags.append("dbt")
    elif schedule["transform"] == "only":
        tags.append("dbt-only")

    dag = create_dag(dag_id, tags=tags)

    elt = BashOperator(
        task_id=schedule['name'],
        bash_command=get_meltano_command(f"schedule run {schedule['name']}"),
        dag=dag,
    )
