from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "update-collection-weights"
tags = ["optimization"]
dag = create_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="0 7 1 * *")

file_name = 'weights_{{ ts_nodash }}.csv'
file_path = f'/tmp/{file_name}'

optimize = BashOperator(
    task_id="optimize",
    bash_command=
    f"gainy_optimize_collections -d $(date '+%Y-%m-%d') -o {file_path}",
    dag=dag)

upload = BashOperator(
    task_id="upload",
    bash_command=
    f"gainy_github_update_file -r gainy-app/gainy -d src/meltano/meltano/data/ticker_collections_weights/{file_name} -s {file_path} --team-reviewers collection-weights-reviewers",
    dag=dag)

cleanup = BashOperator(task_id="cleanup",
                       bash_command=f"rm {file_path} || true",
                       dag=dag)

cleanup >> optimize >> upload >> cleanup
