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
                    max(updated_at)                    as updated_at,
                    sum(quantity_norm * current_price) as actual_value,
                    sum(absolute_gain_1d)              as absolute_gain_1d,
                    sum(absolute_gain_1w)              as absolute_gain_1w,
                    sum(absolute_gain_1m)              as absolute_gain_1m,
                    sum(absolute_gain_3m)              as absolute_gain_3m,
                    sum(absolute_gain_1y)              as absolute_gain_1y,
                    sum(absolute_gain_5y)              as absolute_gain_5y,
                    sum(absolute_gain_total)           as absolute_gain_total
             from (
                      select distinct on (
                          portfolio_expanded_transactions.uniq_id
                          ) profile_holdings.id                                  as holding_id,
                            profile_holdings.profile_id,
                            portfolio_transaction_gains.updated_at,
                            portfolio_expanded_transactions.quantity_norm::numeric,
                            historical_prices_aggregated.adjusted_close::numeric as current_price,
                            portfolio_transaction_gains.absolute_gain_1d::numeric,
                            portfolio_transaction_gains.absolute_gain_1w::numeric,
                            portfolio_transaction_gains.absolute_gain_1m::numeric,
                            portfolio_transaction_gains.absolute_gain_3m::numeric,
                            portfolio_transaction_gains.absolute_gain_1y::numeric,
                            portfolio_transaction_gains.absolute_gain_5y::numeric,
                            portfolio_transaction_gains.absolute_gain_total::numeric
                      from {{ ref('portfolio_transaction_gains') }}
                               join {{ ref('portfolio_expanded_transactions') }}
                                    on portfolio_expanded_transactions.uniq_id = portfolio_transaction_gains.transaction_uniq_id
                               join {{ source ('app', 'portfolio_securities') }}
                                    on portfolio_securities.id = portfolio_expanded_transactions.security_id
                               join {{ ref('historical_prices_aggregated') }}
                                    on historical_prices_aggregated.symbol = portfolio_securities.ticker_symbol and
                                       historical_prices_aggregated.datetime >= now() - interval '1 week'
                               join {{ source ('app', 'profile_holdings') }}
                                    on profile_holdings.profile_id = portfolio_expanded_transactions.profile_id and
                                       profile_holdings.security_id = portfolio_expanded_transactions.security_id
                      order by portfolio_expanded_transactions.uniq_id, historical_prices_aggregated.time desc
                  ) t
             group by t.holding_id
         ),
     long_term_tax_holdings as
         (
             select distinct on (holding_id) holding_id,
                                             ltt_quantity_total::double precision
             from (
                      select profile_holdings.id                                                                                                             as holding_id,
                             quantity_sign,
                             datetime,
                             min(cumsum)
                             over (partition by t.profile_id, t.security_id order by t.quantity_sign, datetime rows between current row and unbounded following) as ltt_quantity_total
                      from (
                               select portfolio_expanded_transactions.profile_id,
                                      security_id,
                                      datetime,
                                      sign(quantity_norm)                                                                                            as quantity_sign,
                                      sum(quantity_norm)
                                      over (partition by security_id, portfolio_expanded_transactions.profile_id order by sign(quantity_norm), datetime) as cumsum
                               from {{ ref('portfolio_expanded_transactions') }}
                                        join {{ source('app', 'portfolio_securities') }}
                                             on portfolio_securities.id = portfolio_expanded_transactions.security_id
                               where portfolio_expanded_transactions.type in ('buy', 'sell')
                                 and portfolio_securities.type in ('mutual fund', 'equity', 'etf')
                           ) t
                                join {{ source('app', 'profile_holdings') }}
                                    on profile_holdings.profile_id = t.profile_id and
                                       profile_holdings.security_id = t.security_id
                  ) t
             where datetime < now() - interval '1 year'
             order by holding_id, quantity_sign desc, datetime desc
         )
select expanded_holdings.holding_id,
       updated_at,
       actual_value::double precision,
       (actual_value / sum(actual_value) over (partition by profile_id))::double precision as value_to_portfolio_value,
       (absolute_gain_1d / (actual_value - absolute_gain_1d))::double precision            as relative_gain_1d,
       (absolute_gain_1w / (actual_value - absolute_gain_1w))::double precision            as relative_gain_1w,
       (absolute_gain_1m / (actual_value - absolute_gain_1m))::double precision            as relative_gain_1m,
       (absolute_gain_3m / (actual_value - absolute_gain_3m))::double precision            as relative_gain_3m,
       (absolute_gain_1y / (actual_value - absolute_gain_1y))::double precision            as relative_gain_1y,
       (absolute_gain_5y / (actual_value - absolute_gain_5y))::double precision            as relative_gain_5y,
       (absolute_gain_total / (actual_value - absolute_gain_total))::double precision      as relative_gain_total,
       absolute_gain_1d::double precision,
       absolute_gain_1w::double precision,
       absolute_gain_1m::double precision,
       absolute_gain_3m::double precision,
       absolute_gain_1y::double precision,
       absolute_gain_5y::double precision,
       absolute_gain_total::double precision,
       coalesce(long_term_tax_holdings.ltt_quantity_total, 0)                         as ltt_quantity_total
from expanded_holdings
left join long_term_tax_holdings on long_term_tax_holdings.holding_id = expanded_holdings.holding_id