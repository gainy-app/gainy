{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "symbol__interest_id" ON {{ this }} (symbol, interest_id)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

SELECT distinct concat(ti.symbol, '_', ii.interest_id)::varchar as id,
                ti.symbol,
                ii.interest_id,
                now() as updated_at
from {{ ref('interest_industries') }} ii
    join {{ ref ('ticker_industries') }} ti on ii.industry_id = ti.industry_id