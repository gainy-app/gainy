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

    
with raw_ticker_collections as 
         (
             SELECT distinct (ticker_collections.symbol || '_' || collections.id::varchar)::varchar as id,
                             tickers.symbol,
                             collections.id as collection_id,
                             ticker_collections.weight::double precision
             from {{ source('gainy', 'ticker_collections') }}
                      join {{ ref('tickers') }} on tickers.symbol = ticker_collections.symbol
                      join {{ ref('collections') }} on collections.name = trim(ticker_collections.ttf_name)
             where collections.personalized = '0'
               and collections.enabled = '1'
               and ticker_collections._sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source ('gainy', 'ticker_collections') }}) - interval '1 minute'
         ),
     collection_weight_sum as
         (
             select collection_id, sum(weight) as weight_sum
             from raw_ticker_collections
             group by collection_id
         ),
     data as
         (
             select collection_id || '_' || symbol as id,
                    collection_id,
                    symbol,
                    (case
                         when collection_weight_sum.weight_sum is null or collection_weight_sum.weight_sum < 1e-3
                             then
                                 ticker_metrics.market_capitalization /
                                 sum(ticker_metrics.market_capitalization) over (partition by collection_id)
                         else
                             weight / collection_weight_sum.weight_sum
                        end)::double precision     as weight,
                    now()                          as updated_at
             from raw_ticker_collections
                      left join {{ ref('ticker_metrics') }} using (symbol)
                      left join collection_weight_sum using (collection_id)
         )
select *
from data
where weight is not null
