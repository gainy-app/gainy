{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'id', true),
      index(this, 'uniq_id', true),
    ]
  )
}}

with normalized_transactions as
         (
             select id,
                    amount,
                    date,
                    name,
                    price,
                    quantity,
                    subtype,
                    type,
                    security_id,
                    profile_id,
                    account_id,
                    abs(quantity) * case when type = 'sell' then -1 else 1 end as quantity_norm
             from {{ source('app', 'profile_portfolio_transactions') }}
         ),
     first_trade_date as
         (
             select distinct on (code) code,
                                       first_value(date)
                                       over (partition by code order by date) as first_trade_date
             from {{ ref('historical_prices') }}
         ),
     mismatched_sell_transactions as
         (
             with expanded_transactions as
                      (
                          select *,
                                 sum(quantity_norm)
                                 over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as rolling_quantity,
                                 first_value(date)
                                 over (partition by profile_id order by date)                                                                     as profile_first_transaction_date
                          from normalized_transactions
                      )
             select distinct on (
                 expanded_transactions.security_id,
                 expanded_transactions.account_id
                 ) null::int                                  as id,
                   'auto0_' || expanded_transactions.account_id || '_' ||
                   expanded_transactions.security_id          as uniq_id,
                   coalesce(ticker_options.last_price, historical_prices.adjusted_close) *
                   abs(rolling_quantity)                      as amount,
                   null::timestamp                            as datetime,
                   'ASSUMPTION BOUGHT ' || abs(rolling_quantity) || ' ' ||
                   coalesce(ticker_options.symbol || ' ' || to_char(ticker_options.expiration_date, 'MM/dd/YYYY') ||
                            ' ' ||
                            ticker_options.strike || ' ' || INITCAP(ticker_options.type), expanded_transactions.name) ||
                   ' @ ' ||
                   coalesce(ticker_options.last_price,
                            historical_prices.adjusted_close) as name,
                   coalesce(ticker_options.last_price,
                            historical_prices.adjusted_close) as price,
                   abs(rolling_quantity)                      as quantity,
                   'buy'                                      as subtype,
                   'buy'                                      as type,
                   expanded_transactions.security_id,
                   expanded_transactions.profile_id,
                   expanded_transactions.account_id,
                   abs(rolling_quantity)                      as quantity_norm
             from expanded_transactions
                      left join {{ ref('portfolio_securities_normalized') }}
                                on portfolio_securities_normalized.id = expanded_transactions.security_id
                      left join first_trade_date
                                on first_trade_date.code = portfolio_securities_normalized.ticker_symbol
                      left join {{ ref('historical_prices') }}
                                on historical_prices.code = portfolio_securities_normalized.ticker_symbol and
                                   (historical_prices.date between expanded_transactions.profile_first_transaction_date - interval '1 week' and expanded_transactions.profile_first_transaction_date or
                                    historical_prices.date = first_trade_date.first_trade_date)
                      left join {{ ref('ticker_options') }}
                                on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
             where rolling_quantity < 0
               and coalesce(ticker_options.contract_name, historical_prices.code) is not null
         ),
     expanded_transactions as
         (
             select *,
                    sum(quantity_norm)
                    over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as rolling_quantity,
                    first_value(date)
                    over (partition by profile_id order by date)                                                                     as profile_first_transaction_date
             from (
                      select account_id, security_id, date, quantity_norm, type, profile_id
                      from normalized_transactions
                      union all
                      select account_id, security_id, null as date, quantity_norm, type, profile_id
                      from mismatched_sell_transactions
                  ) t
         )

select *
from mismatched_sell_transactions

union all

-- mismatched holdings
(
    select distinct on (
        security_id,
        account_id
        ) null::int                                                                    as id,
          'auto1_' || account_id || '_' || security_id                                 as uniq_id,
          coalesce(ticker_options.last_price, historical_prices.adjusted_close) * diff as amount,
          null::timestamp                                                              as datetime,
          'ASSUMPTION BOUGHT ' || diff || ' ' ||
          coalesce(ticker_options.symbol || ' ' || to_char(ticker_options.expiration_date, 'MM/dd/YYYY') || ' ' ||
                   ticker_options.strike || ' ' || INITCAP(ticker_options.type), t.name) || ' @ ' ||
          coalesce(ticker_options.last_price, historical_prices.adjusted_close)        as name,
          coalesce(ticker_options.last_price, historical_prices.adjusted_close)        as price,
          diff                                                                         as quantity,
          'buy'                                                                        as subtype,
          'buy'                                                                        as type,
          security_id,
          profile_id,
          account_id,
          diff                                                                         as quantity_norm
    from (
             select distinct on (
                 profile_holdings_normalized.account_id, profile_holdings_normalized.security_id
                 ) profile_holdings_normalized.quantity,
                   profile_holdings_normalized.name,
                   profile_holdings_normalized.ticker_symbol,
                   profile_holdings_normalized.security_id,
                   profile_holdings_normalized.profile_id,
                   profile_holdings_normalized.account_id,
                   profile_first_transaction_date,
                   profile_holdings_normalized.quantity -
                   coalesce(expanded_transactions.rolling_quantity, 0) as diff
             from {{ ref('profile_holdings_normalized') }}
                      left join expanded_transactions
                                on profile_holdings_normalized.account_id = expanded_transactions.account_id and
                                   profile_holdings_normalized.security_id = expanded_transactions.security_id
             order by profile_holdings_normalized.account_id, profile_holdings_normalized.security_id,
                      expanded_transactions.date desc
         ) t
             left join first_trade_date on first_trade_date.code = ticker_symbol
             left join {{ ref('historical_prices') }}
                       on historical_prices.code = ticker_symbol and
                          (historical_prices.date between profile_first_transaction_date - interval '1 week' and profile_first_transaction_date or
                           historical_prices.date = first_trade_date.first_trade_date)
             join {{ ref('portfolio_securities_normalized') }} on portfolio_securities_normalized.id = t.security_id
             left join {{ ref('ticker_options') }}
                       on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
    where diff > 0
)

union all

select id,
       id || '_' || account_id || '_' || security_id as uniq_id,
       amount,
       date::timestamp                               as datetime,
       name,
       price,
       quantity,
       subtype,
       type,
       security_id,
       profile_id,
       account_id,
       quantity_norm
from normalized_transactions