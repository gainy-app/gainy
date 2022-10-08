# Meltano data pipelines

## Notes
1. `scripts/generate_config.py` generates config for give env on start up. It takes into account either `symbols.$ENV.json` or `exchanges.$ENV.json`.
2. Airflow dags are generated based on [dag files](meltano/orchestrate/dags) directory. Airflow schedules and runs the specified dags via its scheduler and is configured via a web interface.

## Force fetching all prices for symbols:
```postgresql

with latest_jobs as
         (
             select distinct on (
                 job_name
                 ) id,
                   job_name,
                   payload::json as payload
             from meltano.runs
             where job_name like 'eodhistoricaldata-prices-to-postgres-%'
             order by job_name, started_at desc
         ),
     partitions as
         (
             select id,
                    json_array_elements(payload::json -> 'singer_state' -> 'bookmarks' -> 'eod_historical_prices' ->
                                        'partitions') as partition
             from latest_jobs
     ),
     filtered_partitions as
         (
             select id,
                    jsonb_agg(partition) as partitions
             from partitions
             where partition -> 'context' ->> 'symbol' not in (:symbols)
             group by id
     )
update meltano.runs
set payload = jsonb_set(payload::jsonb, array ['singer_state','bookmarks','eod_historical_prices','partitions'],
                        partitions)
from filtered_partitions
where filtered_partitions.id = runs.id;
```
