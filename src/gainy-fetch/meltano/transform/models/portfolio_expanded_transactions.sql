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

-- mismatched sell transactions
(
    select distinct on (
        expanded_transactions.security_id,
        expanded_transactions.account_id
        ) null::int                                                                               as id,
          'auto_' || expanded_transactions.account_id || '_' || expanded_transactions.security_id as uniq_id,
          historical_prices.adjusted_close * abs(rolling_quantity)                                as amount,
          null::timestamp                                                                         as datetime,
          'ASSUMPTION BOUGHT ' || abs(rolling_quantity) || ' ' || portfolio_securities_normalized.name || ' @ ' ||
          historical_prices.adjusted_close                                                        as name,
          historical_prices.adjusted_close                                                        as price,
          abs(rolling_quantity)                                                                   as quantity,
          'buy'                                                                                   as subtype,
          'buy'                                                                                   as type,
          expanded_transactions.security_id,
          expanded_transactions.profile_id,
          expanded_transactions.account_id,
          abs(rolling_quantity)                                                                   as quantity_norm
    from expanded_transactions
             join {{ ref('portfolio_securities_normalized') }} on portfolio_securities_normalized.id = expanded_transactions.security_id
             left join first_trade_date on first_trade_date.code = portfolio_securities_normalized.ticker_symbol
             join {{ ref('historical_prices') }}
                  on historical_prices.code = portfolio_securities_normalized.ticker_symbol and
                     (historical_prices.date between expanded_transactions.profile_first_transaction_date - interval '1 week' and expanded_transactions.profile_first_transaction_date or
                      historical_prices.date = first_trade_date.first_trade_date)
    where rolling_quantity < 0
)

union all

-- mismatched holdings
(
    select distinct on (
        security_id,
        account_id
        ) null::int                                                                                as id,
          'auto_' || account_id || '_' || security_id                                              as uniq_id,
          historical_prices.adjusted_close * diff                                                  as amount,
          null::timestamp                                                                          as datetime,
          'ASSUMPTION BOUGHT ' || diff || ' ' || name || ' @ ' || historical_prices.adjusted_close as name,
          historical_prices.adjusted_close                                                         as price,
          diff                                                                                     as quantity,
          'buy'                                                                                    as subtype,
          'buy'                                                                                    as type,
          security_id,
          profile_id,
          account_id,
          diff                                                                                     as quantity_norm
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
                   profile_holdings_normalized.quantity - coalesce(expanded_transactions.rolling_quantity, 0) as diff
             from {{ ref('profile_holdings_normalized') }}
                      left join expanded_transactions
                                on profile_holdings_normalized.account_id = expanded_transactions.account_id and
                                   profile_holdings_normalized.security_id = expanded_transactions.security_id
             order by profile_holdings_normalized.account_id, profile_holdings_normalized.security_id,
                      expanded_transactions.date desc
         ) t
             left join first_trade_date on first_trade_date.code = ticker_symbol
             join {{ ref('historical_prices') }}
                  on historical_prices.code = ticker_symbol and
                     (historical_prices.date between profile_first_transaction_date - interval '1 week' and profile_first_transaction_date or
                      historical_prices.date = first_trade_date.first_trade_date)
    where diff > 0
)

union all

select id,
       id || '_' || account_id || '_' || security_id as uniq_id,
       amount,
       date::timestamp as datetime,
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