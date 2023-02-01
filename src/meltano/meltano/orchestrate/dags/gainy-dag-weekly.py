from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command, ENV

dag_id = "gainy-dag-weekly"
tags = ["gainy-compute", "debug", "recommendations"]
dag = create_dag(
    dag_id,
    tags=tags,
    is_paused_upon_creation=(ENV != 'production'),
    schedule_interval="0 10 * * 6" if ENV == "production" else "0 11 * * 6")

gainy_recommendation = BashOperator(
    task_id="update-recommendations",
    bash_command=
    "gainy_recommendation --batch_size=15",  # 15 gives the best performance
    dag=dag,
    pool="gainy_recommendation")

upload_to_s3 = BashOperator(task_id="postgres-history-weekly-to-s3",
                            bash_command=get_meltano_command(
                                "schedule run postgres-history-weekly-to-s3"),
                            dag=dag)

upload_to_analytics = BashOperator(
    task_id="postgres-to-analytics-match-score",
    bash_command=get_meltano_command(
        "schedule run postgres-to-analytics-match-score"),
    dag=dag)

gainy_recommendation >> upload_to_s3
gainy_recommendation >> upload_to_analytics
