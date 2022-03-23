{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "symbol__category_id" ON {{ this }} (symbol, category_id)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


select (category_id || '_' || symbol)::varchar as id,
       category_id,
       symbol,
       now()::timestamp                        as updated_at
from {{ ref('ticker_categories_continuous') }}
WHERE sim_dif > 0
