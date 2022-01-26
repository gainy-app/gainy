{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "profile_id__ticker_symbol") }} (profile_id, ticker_symbol)',
    ]
  )
}}

-- TODO uncomment when ticker prices are present
-- with actual_prices as
--          (
--              select distinct on (symbol) symbol, adjusted_close, '0d'::varchar as period
--              from {{ ref('historical_prices_aggregated') }}
--              where ((period = '15min' and datetime > now() - interval '2 hour') or
--                     (period = '1d' and datetime > now() - interval '1 week'))
--                and adjusted_close is not null
--              order by symbol, datetime desc
--          ),
--      expanded_holding_groups as
--          (
--              select profile_id,
--                     ticker_symbol,
--                     max(updated_at)                        as updated_at,
--                     sum(actual_value::numeric)             as actual_value,
--                     sum(value_to_portfolio_value::numeric) as value_to_portfolio_value,
--                     sum(absolute_gain_1d::numeric)         as absolute_gain_1d,
--                     sum(absolute_gain_1w::numeric)         as absolute_gain_1w,
--                     sum(absolute_gain_1m::numeric)         as absolute_gain_1m,
--                     sum(absolute_gain_3m::numeric)         as absolute_gain_3m,
--                     sum(absolute_gain_1y::numeric)         as absolute_gain_1y,
--                     sum(absolute_gain_5y::numeric)         as absolute_gain_5y,
--                     sum(absolute_gain_total::numeric)      as absolute_gain_total,
--                     sum(ltt_quantity_total::numeric)       as ltt_quantity_total
--              from {{ ref('portfolio_holding_gains') }}
--                       join {{ ref('profile_holdings_normalized') }}
--                            on profile_holdings_normalized.holding_id = portfolio_holding_gains.holding_id
--              group by profile_id, ticker_symbol
--          )
-- select profile_id,
--        ticker_symbol,
--        updated_at,
--        actual_value::double precision,
--        value_to_portfolio_value::double precision,
--        ltt_quantity_total::double precision,
--        (absolute_gain_1d / (actual_prices.adjusted_close - absolute_gain_1d))::double precision       as relative_gain_1d,
--        (absolute_gain_1w / (actual_prices.adjusted_close - absolute_gain_1w))::double precision       as relative_gain_1w,
--        (absolute_gain_1m / (actual_prices.adjusted_close - absolute_gain_1m))::double precision       as relative_gain_1m,
--        (absolute_gain_3m / (actual_prices.adjusted_close - absolute_gain_3m))::double precision       as relative_gain_3m,
--        (absolute_gain_1y / (actual_prices.adjusted_close - absolute_gain_1y))::double precision       as relative_gain_1y,
--        (absolute_gain_5y / (actual_prices.adjusted_close - absolute_gain_5y))::double precision       as relative_gain_5y,
--        (absolute_gain_total / (actual_prices.adjusted_close - absolute_gain_total))::double precision as relative_gain_total,
--        absolute_gain_1d::double precision,
--        absolute_gain_1w::double precision,
--        absolute_gain_1m::double precision,
--        absolute_gain_3m::double precision,
--        absolute_gain_1y::double precision,
--        absolute_gain_5y::double precision,
--        absolute_gain_total::double precision
-- from expanded_holding_groups
--         left join actual_prices on actual_prices.symbol = expanded_holding_groups.ticker_symbol
with expanded_holding_groups as
         (
             select profile_id,
                    ticker_symbol,
                    max(updated_at)                        as updated_at,
                    sum(actual_value::numeric)             as actual_value,
                    sum(value_to_portfolio_value::numeric) as value_to_portfolio_value,
                    sum(absolute_gain_total::numeric)      as absolute_gain_total,
                    sum(ltt_quantity_total::numeric)       as ltt_quantity_total
             from {{ ref('portfolio_holding_gains') }}
                      join {{ ref('profile_holdings_normalized') }}
                           on profile_holdings_normalized.holding_id = portfolio_holding_gains.holding_id
             group by profile_id, ticker_symbol
         ),
     expanded_holding_groups_with_gains as
         (
             select distinct on (
                 expanded_holding_groups.profile_id,
                 expanded_holding_groups.ticker_symbol
                 ) expanded_holding_groups.profile_id,
                   expanded_holding_groups.ticker_symbol,
                   expanded_holding_groups.updated_at,
                   expanded_holding_groups.actual_value,
                   expanded_holding_groups.value_to_portfolio_value,
                   expanded_holding_groups.absolute_gain_total,
                   ticker_realtime_metrics.actual_price,
                   ticker_realtime_metrics.relative_daily_change                  as relative_gain_1d,
                   ticker_metrics.price_change_1w                                 as relative_gain_1w,
                   ticker_metrics.price_change_1m                                 as relative_gain_1m,
                   ticker_metrics.price_change_3m                                 as relative_gain_3m,
                   ticker_metrics.price_change_1y                                 as relative_gain_1y,
                   ticker_metrics.price_change_5y                                 as relative_gain_5y,
                   ticker_metrics.price_change_all                                as relative_gain_total,
                   ltt_quantity_total
             from expanded_holding_groups
                      left join {{ ref('tickers') }}
                                on tickers.symbol = expanded_holding_groups.ticker_symbol
                      left join {{ ref('ticker_metrics') }}
                                on ticker_metrics.symbol = tickers.symbol
                      left join {{ ref('ticker_realtime_metrics') }}
                                on ticker_realtime_metrics.symbol = tickers.symbol
                      left join {{ ref('historical_prices_aggregated') }}
                                on historical_prices_aggregated.symbol = expanded_holding_groups.ticker_symbol
                                    and (historical_prices_aggregated.datetime >=
                                         tickers.ipo_date or tickers.ipo_date is null)
                                    and historical_prices_aggregated.period = '1d'
             order by expanded_holding_groups.profile_id, expanded_holding_groups.ticker_symbol,
                      historical_prices_aggregated.datetime desc
         )
select concat(profile_id, '_', ticker_symbol)::varchar as id,
       profile_id,
       ticker_symbol,
       updated_at,
       actual_value::double precision,
       value_to_portfolio_value::double precision,
       ltt_quantity_total::double precision,
       relative_gain_1d::double precision,
       relative_gain_1w::double precision,
       relative_gain_1m::double precision,
       relative_gain_3m::double precision,
       relative_gain_1y::double precision,
       relative_gain_5y::double precision,
       relative_gain_total::double precision,
       (actual_price * (1 - 1 / (1 + relative_gain_1d)))::double precision    as absolute_gain_1d,
       (actual_price * (1 - 1 / (1 + relative_gain_1w)))::double precision    as absolute_gain_1w,
       (actual_price * (1 - 1 / (1 + relative_gain_1m)))::double precision    as absolute_gain_1m,
       (actual_price * (1 - 1 / (1 + relative_gain_3m)))::double precision    as absolute_gain_3m,
       (actual_price * (1 - 1 / (1 + relative_gain_1y)))::double precision    as absolute_gain_1y,
       (actual_price * (1 - 1 / (1 + relative_gain_5y)))::double precision    as absolute_gain_5y,
       (actual_price * (1 - 1 / (1 + relative_gain_total)))::double precision as absolute_gain_total
from expanded_holding_groups_with_gains
