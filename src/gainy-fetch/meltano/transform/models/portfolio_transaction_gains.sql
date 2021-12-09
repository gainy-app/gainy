{{
  config(
    materialized = "incremental",
    unique_key = "transaction_id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'transaction_id', true),
    ]
  )
}}

with relative_data as
         (
             select distinct on (
                 portfolio_expanded_transactions.id
                 ) portfolio_expanded_transactions.id            as transaction_id,
                   historical_prices_aggregated.datetime::timestamp as updated_at,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric * (
                               historical_prices_aggregated.adjusted_close::numeric /
                               first_value(historical_prices_aggregated.adjusted_close::numeric)
                               over (partition by historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 day' PRECEDING) -
                               1
                       )                                        as relative_gain_1d,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric * (
                               historical_prices_aggregated.adjusted_close::numeric /
                               first_value(historical_prices_aggregated.adjusted_close::numeric)
                               over (partition by historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 week' PRECEDING) -
                               1
                       )                                        as relative_gain_1w,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric * (
                               historical_prices_aggregated.adjusted_close::numeric /
                               first_value(historical_prices_aggregated.adjusted_close::numeric)
                               over (partition by historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 month' PRECEDING) -
                               1
                       )                                        as relative_gain_1m,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric * (
                               historical_prices_aggregated.adjusted_close::numeric /
                               first_value(historical_prices_aggregated.adjusted_close::numeric)
                               over (partition by historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '3 months' PRECEDING) -
                               1
                       )                                        as relative_gain_3m,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric * (
                               historical_prices_aggregated.adjusted_close::numeric /
                               first_value(historical_prices_aggregated.adjusted_close::numeric)
                               over (partition by historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 year' PRECEDING) -
                               1
                       )                                        as relative_gain_1y,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric * (
                               historical_prices_aggregated.adjusted_close::numeric /
                               first_value(historical_prices_aggregated.adjusted_close::numeric)
                               over (partition by historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '5 years' PRECEDING) -
                               1
                       )                                        as relative_gain_5y,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric * (
                               historical_prices_aggregated.adjusted_close::numeric /
                               first_value(historical_prices_aggregated.adjusted_close::numeric)
                               over (partition by historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE UNBOUNDED PRECEDING) -
                               1
                       )                                        as relative_gain_total,
                   portfolio_expanded_transactions.quantity_norm::numeric
             from {{ ref('portfolio_expanded_transactions') }}
                      join {{ source('app', 'portfolio_securities') }}
                           on portfolio_securities.id = portfolio_expanded_transactions.security_id
                      join {{ ref('historical_prices_aggregated') }}
                           on historical_prices_aggregated.datetime >= portfolio_expanded_transactions.date and
                              (
                                      (historical_prices_aggregated.period = '15min' and
                                       historical_prices_aggregated.datetime >=
                                       now() - interval '2 day') or
                                      (historical_prices_aggregated.period = '1d' and
                                       historical_prices_aggregated.datetime >=
                                       now() - interval '3 month' - interval '1 week') or
                                      (historical_prices_aggregated.period = '1w' and
                                       historical_prices_aggregated.datetime >=
                                       now() - interval '1 year' - interval '1 week') or
                                      (historical_prices_aggregated.period = '1m' and
                                       historical_prices_aggregated.datetime >=
                                       now() - interval '5 year' - interval '1 week')
                                  ) and
                              historical_prices_aggregated.symbol = portfolio_securities.ticker_symbol
             where portfolio_expanded_transactions.id is not null
               and portfolio_expanded_transactions.type in ('buy', 'sell')
               and portfolio_securities.type in ('mutual fund', 'equity', 'etf')
             order by portfolio_expanded_transactions.id, historical_prices_aggregated.datetime desc
         )
select transaction_id,
       updated_at,
       relative_gain_1d::double precision,
       relative_gain_1w::double precision,
       relative_gain_1m::double precision,
       relative_gain_3m::double precision,
       relative_gain_1y::double precision,
       relative_gain_5y::double precision,
       relative_gain_total::double precision,
       (relative_gain_1d * abs(quantity_norm))::double precision    as absolute_gain_1d,
       (relative_gain_1w * abs(quantity_norm))::double precision    as absolute_gain_1w,
       (relative_gain_1m * abs(quantity_norm))::double precision    as absolute_gain_1m,
       (relative_gain_3m * abs(quantity_norm))::double precision    as absolute_gain_3m,
       (relative_gain_1y * abs(quantity_norm))::double precision    as absolute_gain_1y,
       (relative_gain_5y * abs(quantity_norm))::double precision    as absolute_gain_5y,
       (relative_gain_total * abs(quantity_norm))::double precision as absolute_gain_total
from relative_data