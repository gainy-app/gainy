{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('collection_uniq_id, symbol'),
      index(this, 'id', true),
    ]
  )
}}


select distinct on (
    collection_uniq_id, symbol
    ) profile_id,
      collection_id,
      collection_uniq_id,
      symbol,
      date,
      weight,
      collection_uniq_id || '_' || symbol as id,
      updated_at
from {{ ref('collection_ticker_weights') }}
order by collection_uniq_id, symbol, date desc
