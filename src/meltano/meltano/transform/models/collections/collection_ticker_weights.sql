{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('collection_uniq_id, symbol, date'),
      index('id', true),
    ]
  )
}}

with raw_ticker_collections_weights as materialized
         (
             select collections.id    as collection_id,
                    symbol,
                    ticker_collections_weights.date::date,
                    ticker_collections_weights.weight::numeric,
                    optimized_at::date,
                    _sdc_extracted_at as updated_at
             from {{ source('gainy', 'ticker_collections_weights') }}
                      join {{ ref('collections') }} on collections.name = ticker_collections_weights.ttf_name
             where _sdc_extracted_at > (
                                           select max(_sdc_extracted_at) from {{ source('gainy', 'ticker_collections_weights') }}
                                       ) - interval '1 hour'
         ),
     ticker_collections_weights as materialized
         (
             -- raw_ticker_collections_weights
             select '0_' || collection_id as collection_uniq_id,
                    collection_id,
                    symbol,
                    date,
                    weight,
                    optimized_at,
                    updated_at
             from raw_ticker_collections_weights

             union all

             -- extend raw_ticker_collections_weights until now
             select '0_' || collection_id as collection_uniq_id,
                    collection_id,
                    symbol,
                    dd::date              as date,
                    weight,
                    optimized_at,
                    updated_at
             from (
                      select collection_id, max(date) as date
                      from raw_ticker_collections_weights
                      group by collection_id
                  ) collection_max_date
                      join raw_ticker_collections_weights using (collection_id, date)
                      join generate_series(date, now(), interval '1 month') dd on dd > date

             union all

             -- static weights
             select '0_' || collections.id as collection_uniq_id,
                    collections.id         as collection_id,
                    symbol,
                    dd::date               as date,
                    ticker_collections.weight::numeric,
                    optimized_at::date,
                    _sdc_extracted_at      as updated_at
             from {{ source('gainy', 'ticker_collections') }}
                      join {{ ref('collections') }} on collections.name = ticker_collections.ttf_name
                      join generate_series(date_trunc('month', ticker_collections.date_start::date), now()::date, interval '1 month') dd on true
             where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source('gainy', 'ticker_collections') }}) - interval '1 hour'
               and ticker_collections.weight is not null
         ),
     ticker_collections_next_date as materialized
         (
             select collection_id,
                    date,
                    coalesce(
                                    lag(date) over wnd,
                                    (now() + interval '1 day')::date
                        ) as next_date
             from (
                      select distinct collection_id, date
                      from ticker_collections_weights
                  ) t
                 window wnd as (partition by collection_id order by date desc)
     ),
{% if is_incremental() and var('realtime') %}
     old_stats as materialized
         (
             select collection_uniq_id, max(date) as date
             from {{ this }}
             group by collection_uniq_id
         ),
{% endif %}
     ticker_collections_weights_expanded0 as materialized
         (
             select ticker_collections_weights.collection_uniq_id,
                    ticker_collections_weights.collection_id,
                    ticker_collections_weights.symbol,
                    hp.date,
                    hp.price,
                    ticker_collections_weights.weight,
                    ticker_collections_weights.date as period_id,
                    ticker_collections_weights.optimized_at,
                    greatest(ticker_collections_weights.updated_at,
                             hp.updated_at)         as updated_at
             from ticker_collections_weights
                      join ticker_collections_next_date using (collection_id, date)

{% if is_incremental() and var('realtime') %}
                      left join old_stats using (collection_uniq_id)
{% endif %}

                      join (
                               select symbol,
                                      date,
                                      -- close price of previous trading day
                                      adjusted_close / (1 + relative_daily_gain) as price,
                                      updated_at
                               from {{ ref('historical_prices') }}

                               union all

                               select historical_prices_marked.symbol,
                                      week_trading_sessions_static.date,
                                      -- close price of previous trading day
                                      price_0d as price,
                                      null     as updated_at
                               from {{ ref('week_trading_sessions_static') }}
                                        join {{ ref('historical_prices_marked') }} using (symbol)
                               where week_trading_sessions_static.date > historical_prices_marked.date_0d
                           ) hp
                           on hp.symbol = ticker_collections_weights.symbol
                               and hp.date >= ticker_collections_weights.date
                               and hp.date < ticker_collections_next_date.next_date
             where hp.price is not null
               and hp.price > 0

{% if is_incremental() and var('realtime') %}
               and (old_stats.collection_uniq_id is null or hp.date >= old_stats.date - interval '2 month')
{% endif %}
         ),
     ticker_collections_weights_expanded2 as materialized
         (
             select collection_uniq_id,
                    collection_id,
                    symbol,
                    date,
                    price,
                    first_value(price)
                    over (partition by collection_id, symbol, period_id order by date) as latest_rebalance_price,
                    first_value(weight)
                    over (partition by collection_id, symbol, period_id order by date) as latest_rebalance_weight,
                    period_id,
                    optimized_at,
                    updated_at
             from ticker_collections_weights_expanded0
             where period_id is not null
               and weight is not null
         ),
     ticker_collections_weights_expanded as materialized
         (
             select *,
                    latest_rebalance_weight::numeric * price::numeric / latest_rebalance_price::numeric as weight
             from ticker_collections_weights_expanded2
             where latest_rebalance_price > 0
         ),
     ticker_collections_weights_stats as
         (
             select *,
                    lag(date) over (partition by collection_uniq_id order by date desc) as next_date
             from (
                      select collection_uniq_id,
                             date,
                             sum(weight) as weight_sum
                      from ticker_collections_weights_expanded
                      group by collection_uniq_id, date
                  ) t
         ),
     ticker_collections_weights_normalized as
         (
             select collection_uniq_id,
                    collection_id,
                    symbol,
                    date,
                    weight / weight_sum as weight,
                    price::numeric,
                    latest_rebalance_price,
                    latest_rebalance_weight,
                    period_id,
                    optimized_at,
                    updated_at
             from ticker_collections_weights_expanded
                      join ticker_collections_weights_stats using (collection_uniq_id, date)
         )
select ticker_collections_weights_normalized.*,
       null::int                                           as profile_id,
       collection_uniq_id || '_' || symbol  || '_' || date as id
from ticker_collections_weights_normalized

{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id, symbol, date)
{% endif %}

where date is not null

{% if is_incremental() %}
  and (old_data.collection_uniq_id is null
    or abs(ticker_collections_weights_normalized.weight - old_data.weight) > 1e-6
    or abs(ticker_collections_weights_normalized.price - old_data.price) > 1e-3)
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
