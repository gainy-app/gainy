{{
  config(
    materialized = "incremental",
    unique_key = "holding_id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'holding_id', true),
      index(this, 'ticker_symbol'),
    ]
  )
}}

with first_purchase_date as
         (
             select distinct on (
                 profile_holdings.id
                 ) profile_holdings.id as holding_id,
                   date
             from {{ source('app', 'profile_portfolio_transactions') }}
                      join {{ source('app', 'profile_holdings') }}
                           on profile_holdings.profile_id = profile_portfolio_transactions.profile_id and
                              profile_holdings.security_id = profile_portfolio_transactions.security_id
             order by profile_holdings.id, date
         ),
     next_earnings_date as
         (
             select distinct on (
                 symbol
                 ) symbol,
                   date
             from {{ ref('earnings_trend') }}
             where date >= now()
             order by symbol, date
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
select profile_holdings.id                                    as holding_id,
       base_tickers.symbol                                    as ticker_symbol,
       profile_holdings.account_id,
       first_purchase_date.date::timestamp                    as purchase_date,
       relative_gain_total,
       relative_gain_1d,
       portfolio_holding_gains.value_to_portfolio_value,
       base_tickers.name                                      as ticker_name,
       ticker_metrics.market_capitalization,
       next_earnings_date.date::timestamp                     as next_earnings_date,
       portfolio_securities.type                              as security_type,
       coalesce(long_term_tax_holdings.ltt_quantity_total, 0) as ltt_quantity_total
from {{ source('app', 'profile_holdings') }}
         left join first_purchase_date on first_purchase_date.holding_id = profile_holdings.id
         left join {{ ref('portfolio_holding_gains') }} on portfolio_holding_gains.holding_id = profile_holdings.id
         left join {{ source('app', 'portfolio_securities') }} on portfolio_securities.id = profile_holdings.security_id
         left join {{ ref('base_tickers') }} on base_tickers.symbol = portfolio_securities.ticker_symbol
         left join next_earnings_date on next_earnings_date.symbol = base_tickers.symbol
         left join {{ ref('ticker_metrics') }} on ticker_metrics.symbol = base_tickers.symbol
         left join long_term_tax_holdings on long_term_tax_holdings.holding_id = profile_holdings.id