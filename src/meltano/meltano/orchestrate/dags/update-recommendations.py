from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "update-recommendations"
tags = ["gainy-compute", "debug", "recommendations"]
dag = create_dag(dag_id, tags=tags)

gainy_recommendation = BashOperator(
    task_id="update-recommendations",
    bash_command="gainy_recommendation",
    dag=dag,
    schedule_interval="0 3 * * 0" if ENV == "production" else "0 4 * * 0",
    pool="gainy_recommendation")
