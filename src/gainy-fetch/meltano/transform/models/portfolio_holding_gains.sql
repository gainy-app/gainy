{{
  config(
    materialized = "incremental",
    unique_key = "holding_id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'holding_id', true),
    ]
  )
}}

with expanded_holdings as
         (
             select t.holding_id,
                    min(profile_id)                    as profile_id,
                    min(updated_at)                    as updated_at,
                    min(quantity) * min(current_price) as actual_value,
                    sum(absolute_gain_1w)              as absolute_gain_1w,
                    sum(absolute_gain_1m)              as absolute_gain_1m,
                    sum(absolute_gain_3m)              as absolute_gain_3m,
                    sum(absolute_gain_1y)              as absolute_gain_1y,
                    sum(absolute_gain_5y)              as absolute_gain_5y,
                    sum(absolute_gain_total)           as absolute_gain_total
             from (
                      select distinct on (
                          profile_portfolio_transactions.id
                          ) profile_holdings.id                                                           as holding_id,
                            profile_holdings.profile_id,
                            portfolio_transaction_gains.updated_at,
                            profile_holdings.quantity,
                            first_value(historical_prices.adjusted_close::numeric)
                            over (ORDER BY historical_prices.date desc RANGE INTERVAL '1 week' PRECEDING) as current_price,
                            portfolio_transaction_gains.absolute_gain_1w::numeric,
                            portfolio_transaction_gains.absolute_gain_1m::numeric,
                            portfolio_transaction_gains.absolute_gain_3m::numeric,
                            portfolio_transaction_gains.absolute_gain_1y::numeric,
                            portfolio_transaction_gains.absolute_gain_5y::numeric,
                            portfolio_transaction_gains.absolute_gain_total::numeric
                      from {{ ref('portfolio_transaction_gains') }}
                               join {{ source ('app', 'profile_portfolio_transactions') }}
                                    on profile_portfolio_transactions.id = portfolio_transaction_gains.transaction_id
                               join {{ source ('app', 'portfolio_securities') }}
                                    on portfolio_securities.id = profile_portfolio_transactions.security_id
                               join {{ ref('historical_prices') }}
                                    on historical_prices.code = portfolio_securities.ticker_symbol
                               join {{ source ('app', 'profile_holdings') }}
                                    on profile_holdings.profile_id = profile_portfolio_transactions.profile_id and
                                       profile_holdings.security_id = profile_portfolio_transactions.security_id
                      {% if is_incremental() %}
                               left join {{ this }} old_portfolio_holding_gains
                               on old_portfolio_holding_gains.holding_id = profile_holdings.id
                      where old_portfolio_holding_gains.updated_at is null or portfolio_transaction_gains.updated_at > old_portfolio_holding_gains.updated_at
                      {% endif %}
                      order by profile_portfolio_transactions.id, historical_prices.date desc
                  ) t
             group by t.holding_id
         )
select holding_id,
       updated_at,
       actual_value,
       actual_value / sum(actual_value) over (partition by profile_id)                as value_to_portfolio_value,
       (absolute_gain_1w / (actual_value - absolute_gain_1w))::double precision       as relative_gain_1w,
       (absolute_gain_1m / (actual_value - absolute_gain_1m))::double precision       as relative_gain_1m,
       (absolute_gain_3m / (actual_value - absolute_gain_3m))::double precision       as relative_gain_3m,
       (absolute_gain_1y / (actual_value - absolute_gain_1y))::double precision       as relative_gain_1y,
       (absolute_gain_5y / (actual_value - absolute_gain_5y))::double precision       as relative_gain_5y,
       (absolute_gain_total / (actual_value - absolute_gain_total))::double precision as relative_gain_total,
       absolute_gain_1w::double precision,
       absolute_gain_1m::double precision,
       absolute_gain_3m::double precision,
       absolute_gain_1y::double precision,
       absolute_gain_5y::double precision,
       absolute_gain_total::double precision
from expanded_holdings