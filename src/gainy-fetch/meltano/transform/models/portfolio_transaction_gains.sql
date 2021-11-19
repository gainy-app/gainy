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
                 profile_portfolio_transactions.id
                 ) profile_portfolio_transactions.id as transaction_id,
                   historical_prices.date::timestamp as updated_at,
                   sign(profile_portfolio_transactions.quantity)::numeric * (
                               historical_prices.adjusted_close::numeric /
                               first_value(historical_prices.adjusted_close::numeric)
                               over (partition by historical_prices.code ORDER BY historical_prices.date RANGE INTERVAL '1 week' PRECEDING) -
                               1
                       )                             as relative_gain_1w,
                   sign(profile_portfolio_transactions.quantity)::numeric * (
                               historical_prices.adjusted_close::numeric /
                               first_value(historical_prices.adjusted_close::numeric)
                               over (partition by historical_prices.code ORDER BY historical_prices.date RANGE INTERVAL '1 month' PRECEDING) -
                               1
                       )                             as relative_gain_1m,
                   sign(profile_portfolio_transactions.quantity)::numeric * (
                               historical_prices.adjusted_close::numeric /
                               first_value(historical_prices.adjusted_close::numeric)
                               over (partition by historical_prices.code ORDER BY historical_prices.date RANGE INTERVAL '3 months' PRECEDING) -
                               1
                       )                             as relative_gain_3m,
                   sign(profile_portfolio_transactions.quantity)::numeric * (
                               historical_prices.adjusted_close::numeric /
                               first_value(historical_prices.adjusted_close::numeric)
                               over (partition by historical_prices.code ORDER BY historical_prices.date RANGE INTERVAL '1 year' PRECEDING) -
                               1
                       )                             as relative_gain_1y,
                   sign(profile_portfolio_transactions.quantity)::numeric * (
                               historical_prices.adjusted_close::numeric /
                               first_value(historical_prices.adjusted_close::numeric)
                               over (partition by historical_prices.code ORDER BY historical_prices.date RANGE INTERVAL '5 years' PRECEDING) -
                               1
                       )                             as relative_gain_5y,
                   sign(profile_portfolio_transactions.quantity)::numeric * (
                               historical_prices.adjusted_close::numeric /
                               first_value(historical_prices.adjusted_close::numeric)
                               over (partition by historical_prices.code ORDER BY historical_prices.date RANGE UNBOUNDED PRECEDING) -
                               1
                       )                             as relative_gain_total,
                   profile_portfolio_transactions.quantity::numeric
             from {{ source ('app', 'profile_portfolio_transactions') }}
                 {% if is_incremental() %}
                      left join {{ this }} old_portfolio_transaction_gains
                           on old_portfolio_transaction_gains.transaction_id = profile_portfolio_transactions.id
                 {% endif %}
                      join {{ source ('app', 'portfolio_securities') }} on portfolio_securities.id = profile_portfolio_transactions.security_id
                      join {{ ref('historical_prices') }} on historical_prices.date >= profile_portfolio_transactions.date and
                 {% if is_incremental() %}
                                                             (old_portfolio_transaction_gains.updated_at is null or historical_prices.date > old_portfolio_transaction_gains.updated_at) and
                 {% endif %}
                                                             historical_prices.code = portfolio_securities.ticker_symbol
             where profile_portfolio_transactions.type in ('buy', 'sell')
               and portfolio_securities.type in ('mutual fund', 'equity', 'etf')
             order by profile_portfolio_transactions.id, historical_prices.date desc
         )
select transaction_id,
       updated_at,
       relative_gain_1w::double precision,
       relative_gain_1m::double precision,
       relative_gain_3m::double precision,
       relative_gain_1y::double precision,
       relative_gain_5y::double precision,
       relative_gain_total::double precision,
       (relative_gain_1w * abs(quantity))::double precision    as absolute_gain_1w,
       (relative_gain_1m * abs(quantity))::double precision    as absolute_gain_1m,
       (relative_gain_3m * abs(quantity))::double precision    as absolute_gain_3m,
       (relative_gain_1y * abs(quantity))::double precision    as absolute_gain_1y,
       (relative_gain_5y * abs(quantity))::double precision    as absolute_gain_5y,
       (relative_gain_total * abs(quantity))::double precision as absolute_gain_total
from relative_data