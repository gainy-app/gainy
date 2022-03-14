# Meltano data pipelines

## Notes
1. `scripts/generate_config.py` generates config for give env on start up. It takes into account either `symbols.$ENV.json` or `exchanges.$ENV.json`.
2. Airflow dags are generated based on [dag files](meltano/orchestrate/dags) directory. Airflow schedules and runs the specified dags via its scheduler and is configured via a web interface.
