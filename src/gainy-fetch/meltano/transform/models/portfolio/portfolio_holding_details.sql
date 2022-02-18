{{
  config(
    materialized = "incremental",
    unique_key = "holding_id",
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
                              profile_holdings.security_id = profile_portfolio_transactions.security_id and
                              profile_holdings.account_id = profile_portfolio_transactions.account_id
             order by profile_holdings.id, date
         ),
     next_earnings_date as
         (
             select symbol,
                    min(report_date) as date
             from {{ ref('earnings_history') }}
             where report_date >= now()
             group by symbol
         ),
     long_term_tax_holdings as
         (
             select distinct on (holding_id) holding_id,
                                             ltt_quantity_total::double precision
             from (
                      select profile_holdings.id                                                                                                             as holding_id,
                             quantity_sign,
                             date,
                             min(cumsum)
                             over (partition by t.profile_id, t.security_id order by t.quantity_sign, date rows between current row and unbounded following) as ltt_quantity_total
                      from (
                               select portfolio_expanded_transactions.profile_id,
                                      security_id,
                                      portfolio_expanded_transactions.account_id,
                                      date,
                                      sign(quantity_norm)                                                                                            as quantity_sign,
                                      sum(quantity_norm)
                                      over (partition by security_id, portfolio_expanded_transactions.profile_id order by sign(quantity_norm), date) as cumsum
                               from {{ ref('portfolio_expanded_transactions') }}
                                        join {{ ref('portfolio_securities_normalized') }}
                                             on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                               where portfolio_expanded_transactions.type in ('buy', 'sell')
                           ) t
                               join {{ source('app', 'profile_holdings') }}
                                    on profile_holdings.profile_id = t.profile_id and
                                       profile_holdings.security_id = t.security_id and
                                       profile_holdings.account_id = t.account_id
                  ) t
             where date < now() - interval '1 year'
             order by holding_id, quantity_sign desc, date desc
         )
select profile_holdings_normalized.holding_id,
       portfolio_securities_normalized.original_ticker_symbol as ticker_symbol,
       profile_holdings_normalized.account_id,
       first_purchase_date.date::timestamp                    as purchase_date,
       relative_gain_total,
       relative_gain_1d,
       portfolio_holding_gains.value_to_portfolio_value,
       coalesce(ticker_options.name, base_tickers.name,
                portfolio_securities_normalized.name)         as ticker_name,
       ticker_metrics.market_capitalization,
       next_earnings_date.date::timestamp                     as next_earnings_date,
       portfolio_securities_normalized.type                   as security_type,
       coalesce(long_term_tax_holdings.ltt_quantity_total, 0) as ltt_quantity_total,
       profile_holdings_normalized.name,
       profile_holdings_normalized.quantity,
       (actual_value - absolute_gain_total) / quantity        as avg_cost
from {{ ref('profile_holdings_normalized') }}
         left join first_purchase_date on first_purchase_date.holding_id = profile_holdings_normalized.holding_id
         left join {{ ref('portfolio_holding_gains') }} on portfolio_holding_gains.holding_id = profile_holdings_normalized.holding_id
         left join {{ ref('portfolio_securities_normalized') }} on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
         left join {{ ref('base_tickers') }} on base_tickers.symbol = portfolio_securities_normalized.ticker_symbol
         left join next_earnings_date on next_earnings_date.symbol = base_tickers.symbol
         left join {{ ref('ticker_metrics') }} on ticker_metrics.symbol = base_tickers.symbol
         left join long_term_tax_holdings on long_term_tax_holdings.holding_id = profile_holdings_normalized.holding_id
         left join {{ ref('ticker_options') }} on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
