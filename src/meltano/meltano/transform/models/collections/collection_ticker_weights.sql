{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('collection_uniq_id, symbol, date'),
      index(this, 'id', true),
    ]
  )
}}


with ticker_collections_weights as materialized
         (
             select null::int              as profile_id,
                    '0_' || collections.id as collection_uniq_id,
                    collections.id         as collection_id,
                    symbol,
                    ticker_collections_weights.date::date,
                    ticker_collections_weights.weight::numeric,
                    _sdc_extracted_at      as updated_at
             from {{ source('gainy', 'ticker_collections_weights') }}
                      join {{ ref('collections') }} on collections.name = ticker_collections_weights.ttf_name
             where _sdc_extracted_at > (
                                           select max(_sdc_extracted_at) from {{ source('gainy', 'ticker_collections_weights') }}
                                       ) - interval '1 hour'
         ),
     -- Execution Time: 12525.455 ms
     ticker_collections_weights_expanded as materialized
         (
             select profile_id,
                    collection_uniq_id,
                    collection_id,
                    ticker_collections_weights.symbol,
                    historical_prices.date,
                    ticker_collections_weights.weight,
                    historical_prices.adjusted_close,
                    lag(historical_prices.open)
                    over (partition by collection_id, historical_prices.symbol order by historical_prices.date desc) as next_open,
                    greatest(ticker_collections_weights.updated_at, historical_prices.updated_at)                    as updated_at
             from ticker_collections_weights
                      join {{ ref('historical_prices') }}
                           on historical_prices.symbol = ticker_collections_weights.symbol
                               and historical_prices.date between ticker_collections_weights.date
                                                              and ticker_collections_weights.date + interval '1 month' - interval '1 day'
         ),
     ticker_collections_weights_stats as
         (
             select collection_uniq_id,
                    date,
                    sum(weight) as weight_sum
             from ticker_collections_weights_expanded
             group by collection_uniq_id, date
         ),
     ticker_collections_weights_normalized as
         (
             select profile_id,
                    collection_uniq_id,
                    collection_id,
                    symbol,
                    date,
                    weight / weight_sum   as weight,
                    adjusted_close::numeric,
                    next_open::numeric,
                    updated_at
             from ticker_collections_weights_expanded
                      join ticker_collections_weights_stats using (collection_uniq_id, date)
         )
select ticker_collections_weights_normalized.*,
       collection_uniq_id || '_' || symbol  || '_' || date as id
from ticker_collections_weights_normalized

{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id, symbol, date)
where old_data is null
   or abs(ticker_collections_weights_normalized.weight - old_data.weight) > 1e-6
   or abs(ticker_collections_weights_normalized.adjusted_close - old_data.adjusted_close) > 1e-3
   or abs(ticker_collections_weights_normalized.next_open - old_data.next_open) > 1e-3
{% endif %}

-- TODO make it historical for personalized collections
-- union all
--
-- select profile_id || '_' || collection_id                       as collection_uniq_id,
--        symbol,
--        now()::date                                              as date,
--        profile_id,
--        collection_id,
--        (ticker_metrics.market_capitalization /
--         sum(ticker_metrics.market_capitalization)
--         over (partition by profile_id, collection_id))::numeric as weight
-- from {{ source('app', 'personalized_ticker_collections') }}
--          join {{ ref('ticker_metrics') }} using (symbol)
-- where ticker_metrics.market_capitalization is not null
