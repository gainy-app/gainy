{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, category_id'),
      index('id', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


select (category_id || '_' || symbol)::varchar as id,
       category_id,
       symbol,
       rank,
       now()::timestamp                        as updated_at
from {{ ref('ticker_categories_continuous') }}
WHERE sim_dif > 0
