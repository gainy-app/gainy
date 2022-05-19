from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command

dag_id = "debug-restore-views"
tags = ["meltano", "postgres", "dbt"]
dag = create_dag(dag_id, tags=tags)

generate_rules = BashOperator(
    task_id="generate_rules",
    bash_command=
    f"cd {MELTANO_PROJECT_ROOT}; ( cd scripts && /usr/local/bin/python3 generate_rules_sql.py )",
    dag=dag)

dbt = BashOperator(
    task_id="dbt",
    bash_command=get_meltano_command("invoke dbt run --model tag:view"),
    dag=dag)

generate_rules >> dbt
