from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, get_schedules

DEFAULT_TAGS = ["meltano", "debug"]

schedules = get_schedules()

tags = DEFAULT_TAGS.copy() + ["eodhistoricaldata-prices", "postgres"]
dag_id = f"debug-eodhistoricaldata-prices-to-postgres"
dag = create_dag(dag_id, tags=tags)

for schedule in schedules:
    if not schedule['name'].startswith("eodhistoricaldata-prices-to-postgres"):
        continue

    command = BashOperator(
        task_id=schedule['name'],
        bash_command=get_meltano_command(f"schedule run {schedule['name']}"),
        dag=dag,
    )
