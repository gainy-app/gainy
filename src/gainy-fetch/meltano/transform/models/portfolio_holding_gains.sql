{{
  config(
    materialized = "incremental",
    unique_key = "holding_id",
    post_hook=[
      index(this, 'holding_id', true),
    ]
  )
}}


with actual_prices as
         (
             select distinct on (symbol) symbol, adjusted_close, '0d'::varchar as period
             from {{ ref('historical_prices_aggregated') }}
             where ((period = '15min' and datetime > now() - interval '2 hour') or
                    (period = '1d' and datetime > now() - interval '1 week'))
               and adjusted_close is not null
             order by symbol, datetime desc
         ),
--      first_profile_security_trade_date as (
--          select profile_id,
--                 security_id,
--                 min(date) as date
--          from {{ source('app', 'profile_portfolio_transactions') }}
--          group by profile_id, security_id
--      ),
--      first_profile_trade_date as (
--          select profile_id,
--                 min(date) as date
--          from {{ source('app', 'profile_portfolio_transactions') }}
--          group by profile_id
--      ),
     relative_data as
         (
             select distinct on (
                 profile_holdings.id
                 ) profile_holdings.id                                                                  as holding_id,
                   profile_holdings.profile_id,
                   now()::timestamp                                                                     as updated_at,
                   coalesce(actual_prices.adjusted_close,
                            ticker_options.last_price,
                            historical_prices_aggregated.adjusted_close)::numeric                       as actual_price,

                   (coalesce(actual_prices.adjusted_close,
                             ticker_options.last_price * 100,
                             historical_prices_aggregated.adjusted_close) * quantity)::double precision as actual_value,

                   case
                       when ticker_options.contract_name is null
                           then (
                                   actual_prices.adjusted_close::numeric /
                                   first_value(
                                   historical_prices_aggregated.adjusted_close::numeric)
                                   over (partition by profile_holdings.id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '0 day' PRECEDING) -
                                   1)
                       else 0 end                                                                       as relative_gain_1d,
                   case
                       when ticker_options.contract_name is null
                           then (
                                   actual_prices.adjusted_close::numeric /
                                   first_value(
                                   historical_prices_aggregated.adjusted_close::numeric)
                                   over (partition by profile_holdings.id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 week' PRECEDING) -
                                   1)
                       else 0 end                                                                       as relative_gain_1w,
                   case
                       when ticker_options.contract_name is null
                           then (
                                   actual_prices.adjusted_close::numeric /
                                   first_value(
                                   historical_prices_aggregated.adjusted_close::numeric)
                                   over (partition by profile_holdings.id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 month' PRECEDING) -
                                   1)
                       else 0 end                                                                       as relative_gain_1m,
                   case
                       when ticker_options.contract_name is null
                           then (
                                   actual_prices.adjusted_close::numeric /
                                   first_value(
                                   historical_prices_aggregated.adjusted_close::numeric)
                                   over (partition by profile_holdings.id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '3 months' PRECEDING) -
                                   1)
                       else 0 end                                                                       as relative_gain_3m,
                   case
                       when ticker_options.contract_name is null
                           then (
                                   actual_prices.adjusted_close::numeric /
                                   first_value(
                                   historical_prices_aggregated.adjusted_close::numeric)
                                   over (partition by profile_holdings.id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 year' PRECEDING) -
                                   1)
                       else 0 end                                                                       as relative_gain_1y,
                   case
                       when ticker_options.contract_name is null
                           then (
                                   actual_prices.adjusted_close::numeric /
                                   first_value(
                                   historical_prices_aggregated.adjusted_close::numeric)
                                   over (partition by profile_holdings.id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '5 years' PRECEDING) -
                                   1)
                       else 0
                       end                                                                              as relative_gain_5y,
                   case
                       when ticker_options.contract_name is null
                           then (
                                   actual_prices.adjusted_close::numeric /
                                   first_value(
                                   historical_prices_aggregated.adjusted_close::numeric)
                                   over (partition by profile_holdings.id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE UNBOUNDED PRECEDING) -
                                   1)
                       else 0
                       end                                                                              as relative_gain_total,
                   profile_holdings.quantity::numeric
             from {{ source('app', 'profile_holdings') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = profile_holdings.security_id
--                       left join first_profile_security_trade_date
--                                 on first_profile_security_trade_date.profile_id = profile_holdings.profile_id
--                                     and first_profile_security_trade_date.security_id = profile_holdings.security_id
--                       left join first_profile_trade_date
--                                 on first_profile_trade_date.profile_id = profile_holdings.profile_id
                      left join tickers
                                on tickers.symbol = portfolio_securities_normalized.ticker_symbol
                      left join {{ ref('historical_prices_aggregated') }}
                                on historical_prices_aggregated.symbol = portfolio_securities_normalized.ticker_symbol
--                                     and (historical_prices_aggregated.datetime >=
--                                          coalesce(first_profile_security_trade_date.date, first_profile_trade_date.date)
--                                         or (first_profile_security_trade_date.date is null and
--                                             first_profile_trade_date.date is null))
                                    and (historical_prices_aggregated.datetime >=
                                         tickers.ipo_date or tickers.ipo_date is null)
                                    and historical_prices_aggregated.period = '1d'
--                                     and (
--                                            (historical_prices_aggregated.period = '1d' and
--                                             historical_prices_aggregated.datetime >=
--                                             now() - interval '3 month' - interval '1 week') or
--                                            (historical_prices_aggregated.period = '1w' and
--                                             historical_prices_aggregated.datetime >=
--                                             now() - interval '1 year' - interval '1 week') or
--                                            (historical_prices_aggregated.period = '1m' and
--                                             historical_prices_aggregated.datetime >=
--                                             now() - interval '5 year' - interval '1 week')
--                                        )
                      left join actual_prices
                                on actual_prices.symbol = portfolio_securities_normalized.original_ticker_symbol
                      left join {{ ref('ticker_options') }}
                                on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
             where (historical_prices_aggregated.symbol is not null or ticker_options.contract_name is not null)
               and historical_prices_aggregated.datetime < now()::date
             order by profile_holdings.id, historical_prices_aggregated.datetime desc
         ),
     long_term_tax_holdings as
         (
             select distinct on (holding_id) holding_id,
                                             ltt_quantity_total::double precision
             from (
                      select profile_holdings.id                                                                                                                 as holding_id,
                             quantity_sign,
                             datetime,
                             min(cumsum)
                             over (partition by t.profile_id, t.security_id order by t.quantity_sign, datetime rows between current row and unbounded following) as ltt_quantity_total
                      from (
                               select portfolio_expanded_transactions.profile_id,
                                      security_id,
                                      datetime,
                                      sign(quantity_norm)                                                                                                as quantity_sign,
                                      sum(quantity_norm)
                                      over (partition by security_id, portfolio_expanded_transactions.profile_id order by sign(quantity_norm), datetime) as cumsum
                               from {{ ref('portfolio_expanded_transactions') }}
                               where portfolio_expanded_transactions.type in ('buy', 'sell')
                                 and portfolio_expanded_transactions.id is not null
                           ) t
                               join {{ source('app', 'profile_holdings') }}
                                    on profile_holdings.profile_id = t.profile_id and
                                       profile_holdings.security_id = t.security_id
                  ) t
             where datetime < now() - interval '1 year'
             order by holding_id, quantity_sign desc, datetime desc
         )
select relative_data.holding_id,
       updated_at,
       actual_value,
       (actual_value / sum(actual_value) over (partition by profile_id))::double precision as value_to_portfolio_value,
       relative_gain_1d::double precision,
       relative_gain_1w::double precision,
       relative_gain_1m::double precision,
       relative_gain_3m::double precision,
       relative_gain_1y::double precision,
       relative_gain_5y::double precision,
       relative_gain_total::double precision,
       (actual_price * (1 - 1 / (1 + relative_gain_1d)) * quantity)::double precision      as absolute_gain_1d,
       (actual_price * (1 - 1 / (1 + relative_gain_1w)) * quantity)::double precision      as absolute_gain_1w,
       (actual_price * (1 - 1 / (1 + relative_gain_1m)) * quantity)::double precision      as absolute_gain_1m,
       (actual_price * (1 - 1 / (1 + relative_gain_3m)) * quantity)::double precision      as absolute_gain_3m,
       (actual_price * (1 - 1 / (1 + relative_gain_1y)) * quantity)::double precision      as absolute_gain_1y,
       (actual_price * (1 - 1 / (1 + relative_gain_5y)) * quantity)::double precision      as absolute_gain_5y,
       (actual_price * (1 - 1 / (1 + relative_gain_total)) * quantity)::double precision   as absolute_gain_total,
       coalesce(long_term_tax_holdings.ltt_quantity_total, 0)                              as ltt_quantity_total
from relative_data
         left join long_term_tax_holdings on long_term_tax_holdings.holding_id = relative_data.holding_id
