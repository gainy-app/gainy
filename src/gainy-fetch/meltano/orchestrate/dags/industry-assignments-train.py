from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "industry-assignments-train"
tags = ["gainy-compute", "mlflow", "train"]
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval="0 1 * * 0",
                 is_paused_upon_creation=False)

industry_assignments_train = BashOperator(
    task_id="industry-assignments-train",
    bash_command="gainy_industry_assignment train",
    dag=dag)
