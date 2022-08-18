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
     ticker_collections_weights_expanded0 as materialized
         (
             select profile_id,
                    collection_uniq_id,
                    collection_id,
                    ticker_collections_weights.symbol,
                    historical_prices.date,
                    historical_prices.adjusted_close,
                    historical_prices.open                                               as price,
                    first_value(open)
                    over (partition by ticker_collections_weights.collection_id,
                        ticker_collections_weights.symbol,
                        ticker_collections_weights.date order by historical_prices.date) as latest_rebalance_price,
                    first_value(weight)
                    over (partition by ticker_collections_weights.collection_id,
                        ticker_collections_weights.symbol,
                        ticker_collections_weights.date order by historical_prices.date) as latest_rebalance_weight,
                    greatest(ticker_collections_weights.updated_at,
                             historical_prices.updated_at)                               as updated_at
             from ticker_collections_weights
                      join {{ ref('historical_prices') }}
                           on historical_prices.symbol = ticker_collections_weights.symbol
                               and historical_prices.date between ticker_collections_weights.date
                                  and ticker_collections_weights.date + interval '1 month' - interval '1 day'
             where historical_prices.open > 0
         ),
     ticker_collections_weights_expanded as materialized
         (
             select *,
                    latest_rebalance_weight * price / latest_rebalance_price as weight
             from ticker_collections_weights_expanded0
             where latest_rebalance_price > 0
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
                    weight / weight_sum as weight,
                    adjusted_close::numeric,
                    price::numeric,
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
   or abs(ticker_collections_weights_normalized.price - old_data.price) > 1e-3
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
