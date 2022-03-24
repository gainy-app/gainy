{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "industry_id__symbol" ON {{ this }} (industry_id, symbol)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


select id,
       symbol,
       industry_id,
       industry_order,
       updated_at
from {{ ref('ticker_industries_continuous') }}
where similarity > 0.5
