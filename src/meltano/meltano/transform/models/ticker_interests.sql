{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, interest_id'),
      index('id', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


select (interest_id || '_' || symbol)::varchar as id,
       interest_id,
       symbol,
       rank,
       sim_dif,
       now()::timestamp                        as updated_at
from {{ ref('ticker_interests_continuous') }}
WHERE sim_dif > 0 
  and rank <= 3
