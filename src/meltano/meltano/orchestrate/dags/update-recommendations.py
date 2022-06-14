from airflow.operators.bash import BashOperator
from common import create_dag, ENV

dag_id = "update-recommendations"
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
