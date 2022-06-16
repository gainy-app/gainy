from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, ENV

dag_id = "gainy-dag-weekly"
tags = ["gainy-compute", "debug", "recommendations"]
dag = create_dag(
    dag_id,
    tags=tags,
    is_paused_upon_creation=(ENV != 'production'),
    schedule_interval="0 4 * * 0" if ENV == "production" else "0 5 * * 0")

gainy_recommendation = BashOperator(task_id="update-recommendations",
                                    bash_command="gainy_recommendation",
                                    dag=dag,
                                    pool="gainy_recommendation")

upload_to_s3 = BashOperator(task_id="postgres-history-weekly-to-s3",
                            bash_command=get_meltano_command(
                                "schedule run postgres-history-weekly-to-s3"),
                            dag=dag)

gainy_recommendation >> upload_to_s3
