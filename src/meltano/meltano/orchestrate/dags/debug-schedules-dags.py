from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, get_schedules

DEFAULT_TAGS = ["meltano", "debug"]

schedules = get_schedules()

debug_schedule_patterns = [
    "coingecko-to-postgres",
    "eodhistoricaldata-prices-to-postgres",
    "polygon-to-postgres",
    "postgres-to-analytics",
    "postgres-to-search",
]

for pattern in debug_schedule_patterns:
    tags = DEFAULT_TAGS.copy() + pattern.split("-to-")
    dag_id = f"debug-{pattern}"
    dag = create_dag(dag_id, tags=tags)

    for schedule in schedules:
        if not schedule['name'].startswith(pattern):
            continue

        generate_meltano_config = BashOperator(
            task_id="generate_meltano_config",
            bash_command=get_meltano_command(
                "/usr/local/bin/python scripts/generate_meltano_config.py"),
            dag=dag)

        command = BashOperator(
            task_id=schedule['name'],
            bash_command=get_meltano_command(
                f"schedule run {schedule['name']}"),
            dag=dag,
        )

        generate_meltano_config >> command

    globals()[dag_id] = dag
