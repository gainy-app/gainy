{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook = [
      pk('symbol, collection_id'),
      index(this, 'id', true),
      index(this, 'collection_id', false),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
      'update {{ ref("collections") }} c set size = collection_sizes.size from (select collection_id, count(*) as size from {{this}} GROUP BY collection_id) collection_sizes where collection_sizes.collection_id = c.id',
    ]
  )
}}

SELECT distinct (ticker_collections.symbol || '_' || collections.id::varchar)::varchar as id,
                tickers.symbol,
                collections.id as collection_id,
                ticker_collections.weight::float,
                NOW() as updated_at
from {{ source('gainy', 'ticker_collections') }}
         join {{ ref('tickers') }} on tickers.symbol = ticker_collections.symbol
         join {{ ref('collections') }} on collections.name = trim(ticker_collections.ttf_name)
where collections.personalized = '0'
  and ticker_collections._sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source ('gainy', 'ticker_collections') }}) - interval '1 minute'
