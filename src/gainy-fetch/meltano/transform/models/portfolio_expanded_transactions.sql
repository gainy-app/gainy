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
                    quantity * case when type = 'sell' then -1 else 1 end as quantity_norm
             from {{ source('app', 'profile_portfolio_transactions') }}
         ),
     expanded_transactions as
         (
             select *,
                    sum(quantity_norm)
                    over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as rolling_quantity,
                    first_value(date)
                    over (partition by profile_id order by date)                                                                     as profile_first_transaction_date
             from normalized_transactions
         ),
     first_trade_date as
         (
             select distinct on (code) code,
                                       first_value(date)
                                       over (partition by code order by date) as first_trade_date
             from {{ ref('historical_prices') }}
         )

    (
        select distinct on (
            security_id,
            account_id
            ) null as id,
              'auto_' || account_id || '_' || security_id                                             as uniq_id,
              historical_prices.adjusted_close * abs(rolling_quantity)                             as amount,
              historical_prices.date                                                               as date,
              'AUTO BOUGHT ' || abs(rolling_quantity) || ' ' || portfolio_securities.ticker_symbol as name,
              historical_prices.adjusted_close                                                     as price,
              abs(rolling_quantity)                                                                as quantity,
              'buy'                                                                                as subtype,
              'buy'                                                                                as type,
              security_id,
              profile_id,
              account_id,
              abs(rolling_quantity)                                                                as quantity_norm
        from expanded_transactions
                 join {{ source('app', 'portfolio_securities') }} on portfolio_securities.id = expanded_transactions.security_id
                 left join first_trade_date on first_trade_date.code = portfolio_securities.ticker_symbol
                 join {{ ref('historical_prices') }}
                      on historical_prices.code = portfolio_securities.ticker_symbol and
                         (historical_prices.date between expanded_transactions.profile_first_transaction_date - interval '1 week' and expanded_transactions.profile_first_transaction_date or
                          historical_prices.date = first_trade_date.first_trade_date)
        where rolling_quantity < 0
    )

union all

select id,
       id || '_' || account_id || '_' || security_id as uniq_id,
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
       quantity_norm
from normalized_transactions