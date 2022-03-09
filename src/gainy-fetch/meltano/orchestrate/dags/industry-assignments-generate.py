from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "industry-assignments-generator"
tags = ["gainy-compute", "mlflow", "inference", "debug"]
dag = create_dag(dag_id, tags=tags)

industry_assignments_generator = BashOperator(
    task_id="industry-assignments-generator",
    bash_command="gainy_industry_assignment predict",
    skip_exit_code=-9,
    dag=dag)
