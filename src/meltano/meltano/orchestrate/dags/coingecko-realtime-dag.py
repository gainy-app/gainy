from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, ENV

dag_id = "coingecko-realtime-dag"
tags = ["meltano"]
schedule_interval = "*/15 * * * *" if ENV == "production" else "*/60 * * * *"
is_paused_upon_creation = ENV != "production"
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval=schedule_interval,
                 is_paused_upon_creation=is_paused_upon_creation)

coingecko_realtime = BashOperator(
    task_id="coingecko-realtime",
    bash_command=get_meltano_command(
        "schedule run coingecko-realtime-to-postgres --force"),
    dag=dag)
