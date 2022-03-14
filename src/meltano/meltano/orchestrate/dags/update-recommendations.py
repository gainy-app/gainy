from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "debug-update-recommendations"
tags = ["gainy-compute", "debug", "recommendations"]
dag = create_dag(dag_id, tags=tags)

industry_assignments_train = BashOperator(task_id="update-recommendations",
                                          bash_command="gainy_recommendation",
                                          dag=dag)
