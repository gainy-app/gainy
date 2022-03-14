from airflow.operators.bash import BashOperator
from common import create_dag, get_meltano_command

models = " ".join([
    'deployment_metadata',
    'industry_median_chart',
    'historical_prices_aggregated',
    'portfolio_chart',
    'portfolio_expanded_transactions',
    'portfolio_gains',
    'portfolio_holding_chart',
    'portfolio_holding_details',
    'portfolio_holding_gains',
    'portfolio_holding_group_details',
    'portfolio_holding_group_gains',
    'portfolio_transaction_chart',
    'portfolio_transaction_gains',
    'ticker_realtime_metrics',
])

vars = '{"realtime": true}'
dag_id = "realtime-dbt-dag"
tags = ["meltano", "dbt"]
dag = create_dag(dag_id,
                 tags=tags,
                 schedule_interval="*/5 * * * *",
                 is_paused_upon_creation=False)

dbt = BashOperator(task_id="dbt-realtime",
                   bash_command=get_meltano_command(
                       f"invoke dbt run --vars '{vars}' --model {models}"),
                   dag=dag,
                   pool="dbt")
