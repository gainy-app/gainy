{{
  config(
    materialized = "incremental",
    tags = ["realtime"],
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
                    date_trunc('month', ticker_collections_weights.date::date) as date,
                    ticker_collections_weights.weight::numeric,
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
             select null::int             as profile_id,
                    '0_' || collection_id as collection_uniq_id,
                    collection_id,
                    symbol,
                    date,
                    weight,
                    updated_at
             from raw_ticker_collections_weights

             union all

             -- extend raw_ticker_collections_weights until now
             select null::int             as profile_id,
                    '0_' || collection_id as collection_uniq_id,
                    collection_id,
                    symbol,
                    dd::date              as date,
                    weight,
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
             select null::int              as profile_id,
                    '0_' || collections.id as collection_uniq_id,
                    collections.id         as collection_id,
                    symbol,
                    dd::date               as date,
                    ticker_collections.weight::numeric,
                    _sdc_extracted_at      as updated_at
             from {{ source('gainy', 'ticker_collections') }}
                      join {{ ref('collections') }} on collections.name = ticker_collections.ttf_name
                      join generate_series(date_trunc('month', ticker_collections.date_start::date), now()::date, interval '1 month') dd on true
             where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source('gainy', 'ticker_collections') }}) - interval '1 hour'
               and ticker_collections.weight is not null
         ),
     ticker_collections_next_date as materialized
         (
             select collection_uniq_id,
                    symbol,
                    min(week_trading_sessions_static.date) as date
             from (
                      select collection_uniq_id, symbol, max(date) as date
                      from ticker_collections_weights
                      group by collection_uniq_id, symbol
                  ) collection_symbol_dates
                      join (
                               select collection_uniq_id, max(date) as date
                               from ticker_collections_weights
                               group by collection_uniq_id
                           ) collection_dates using (collection_uniq_id)
                      join {{ ref('historical_prices_marked') }} using (symbol)
                      join {{ ref('week_trading_sessions_static') }} using (symbol)
             where week_trading_sessions_static.date > historical_prices_marked.date_0d
               and collection_symbol_dates.date >= collection_dates.date
             group by collection_uniq_id, symbol
         ),
{% if is_incremental() and var('realtime') %}
     old_stats as materialized
         (
             select collection_uniq_id, symbol, max(date) as date
             from {{ this }}
             group by collection_uniq_id, symbol
         ),
{% endif %}
     ticker_collections_weights_expanded0 as materialized
         (
             select distinct on (
                 ticker_collections_weights.collection_uniq_id,
                 ticker_collections_weights.symbol,
                 historical_prices.date
                 ) ticker_collections_weights.profile_id,
                   ticker_collections_weights.collection_uniq_id,
                   ticker_collections_weights.collection_id,
                   ticker_collections_weights.symbol,
                   historical_prices.date,
                   historical_prices.adjusted_close       as price, -- TODO a lot of logics depends on this price to be the "close price of previous trading day"
                   case
                       when historical_prices.date >= ticker_collections_weights.date
                           then ticker_collections_weights.weight
                       end                                as weight,
                   case
                       when historical_prices.date >= ticker_collections_weights.date
                           then ticker_collections_weights.date
                       end                                as period_id,
                   greatest(ticker_collections_weights.updated_at,
                            historical_prices.updated_at) as updated_at
             from ticker_collections_weights
{% if is_incremental() and var('realtime') %}
                      left join old_stats using (collection_uniq_id, symbol)
{% endif %}

                      join {{ ref('historical_prices') }}
                           on historical_prices.symbol = ticker_collections_weights.symbol
                               and historical_prices.date between ticker_collections_weights.date - interval '1 week'
                                  and ticker_collections_weights.date + interval '1 month' - interval '1 day'


             where historical_prices.adjusted_close > 0

{% if is_incremental() and var('realtime') %}
               and (old_stats.collection_uniq_id is null or historical_prices.date >= old_stats.date - interval '2 month')
{% endif %}

             order by ticker_collections_weights.collection_uniq_id,
                      ticker_collections_weights.symbol,
                      historical_prices.date,
                      ticker_collections_weights.date
         ),
     ticker_collections_weights_expanded1 as materialized
         (
             select profile_id,
                    collection_uniq_id,
                    collection_id,
                    symbol,
                    date,
                    price,
                    coalesce(lag(weight) over (partition by collection_id,symbol order by date desc),
                             weight)    as weight,
                    coalesce(lag(period_id) over (partition by collection_id,symbol order by date desc),
                             period_id) as period_id,
                    updated_at
             from ticker_collections_weights_expanded0
         ),
     ticker_collections_weights_expanded2 as materialized
         (
             select profile_id,
                    collection_uniq_id,
                    collection_id,
                    symbol,
                    date,
                    price,
                    first_value(price)
                    over (partition by collection_id, symbol, period_id order by date) as latest_rebalance_price,
                    first_value(weight)
                    over (partition by collection_id, symbol, period_id order by date) as latest_rebalance_weight,
                    updated_at
             from ticker_collections_weights_expanded1
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
             select t.profile_id,
                    t.collection_uniq_id,
                    t.collection_id,
                    t.symbol,
                    coalesce(t.date, ticker_collections_next_date.date) as date,
                    t.weight,
                    t.price,
                    t.updated_at
             from (
                      select profile_id,
                             collection_uniq_id,
                             collection_id,
                             symbol,
                             next_date           as date,
                             weight / weight_sum as weight,
                             price::numeric,
                             updated_at
                      from ticker_collections_weights_expanded
                               join ticker_collections_weights_stats using (collection_uniq_id, date)
                  ) t
             left join ticker_collections_next_date using (symbol, collection_uniq_id)
         )
select ticker_collections_weights_normalized.*,
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
