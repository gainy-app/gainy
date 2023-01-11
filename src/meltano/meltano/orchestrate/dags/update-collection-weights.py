from airflow.operators.bash import BashOperator
from common import create_dag

dag_id = "update-collection-weights"
tags = ["optimization"]
dag = create_dag(dag_id,
                 tags=tags,
                 is_paused_upon_creation=True,
                 schedule_interval="0 7 1 * *")

file_name = 'weights_{{ next_execution_date.strftime("%Y%m%dT%H%M%S") }}.csv'
file_path = f'/tmp/{file_name}'

sysinfo = BashOperator(
    task_id="sysinfo",
    bash_command="/venv/bin/python --version && /venv/bin/python -mpip freeze",
    dag=dag)

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

cleanup_pre = BashOperator(task_id="cleanup_pre",
                           bash_command=f"rm {file_path} || true",
                           dag=dag)

cleanup_post = BashOperator(task_id="cleanup_post",
                            bash_command=f"rm {file_path} || true",
                            dag=dag)

sysinfo >> cleanup_pre >> optimize >> upload >> cleanup_post
