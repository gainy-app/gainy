from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "debug-sync-profiles-analytics-attributes"
tags = ["gainy-compute", "debug"]
dag = create_dag(dag_id, tags=tags)

industry_assignments_generator = BashOperator(
    task_id="sync-profiles-analytics-attributes",
    bash_command="gainy_sync_profiles_analytics_attributes",
    dag=dag)
