{{
  config(
    materialized = "incremental",
    unique_key = "uniq_id",
    tags = ["realtime"],
    post_hook=[
      index(this, 'id', true),
      index(this, 'uniq_id', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
      'delete from {{this}} where account_id not in (select id from app.profile_portfolio_accounts)',
      fk(this, 'account_id', 'app', 'profile_portfolio_accounts', 'id')
    ]
  )
}}

/* plaid transaction are very inaccurate, probably we don't need this at all */

with robinhood_options as (
    select profile_portfolio_transactions.profile_id,
           sum(mod(abs(profile_portfolio_transactions.quantity)::int, 100)) as quantity_module_sum
    from {{ source('app', 'profile_portfolio_transactions') }}
        join {{ source('app', 'portfolio_securities') }} on portfolio_securities.id = profile_portfolio_transactions.security_id
        left join {{ source('app', 'profile_plaid_access_tokens') }} on profile_plaid_access_tokens.id = profile_portfolio_transactions.plaid_access_token_id
        left join {{ source('app', 'plaid_institutions') }} on plaid_institutions.id = profile_plaid_access_tokens.institution_id
    where portfolio_securities.type = 'derivative'
      and plaid_institutions.ref_id = 'ins_54'
    group by profile_portfolio_transactions.profile_id
),
     normalized_transactions as
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
     first_trade_date as ( select code, min(date) as first_trade_date from {{ ref('historical_prices') }} group by code ),
     mismatched_sell_transactions as
         (
             with expanded_transactions0 as
                      (
                          select *,
                                 sum(quantity_norm)
                                 over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as rolling_quantity,
                                 first_value(date)
                                 over (partition by profile_id order by date)                                                                     as profile_first_transaction_date
                          from normalized_transactions
                      ),
                  expanded_transactions as
                      (
                          select security_id,
                                 account_id,
                                 min(profile_id)                     as profile_id,
                                 min(name)                           as name,
                                 min(profile_first_transaction_date) as profile_first_transaction_date,
                                 min(rolling_quantity)               as rolling_quantity
                          from expanded_transactions0
                          group by security_id, account_id
                      ),
                  expanded_transactions_with_price as
                      (
                          select distinct on (
                              expanded_transactions.security_id,
                              expanded_transactions.account_id
                              ) expanded_transactions.security_id,
                                expanded_transactions.account_id,
                                expanded_transactions.profile_id,
                                expanded_transactions.name,
                                expanded_transactions.rolling_quantity,
                                historical_prices.adjusted_close
                          from expanded_transactions
                                   left join {{ ref('portfolio_securities_normalized') }}
                                             on portfolio_securities_normalized.id = expanded_transactions.security_id
                                   left join first_trade_date
                                             on first_trade_date.code = portfolio_securities_normalized.ticker_symbol
                                   left join {{ ref('historical_prices') }}
                                             on historical_prices.code =
                                                portfolio_securities_normalized.ticker_symbol
                                                 and
                                                (historical_prices.date between expanded_transactions.profile_first_transaction_date - interval '1 week'
                                                     and expanded_transactions.profile_first_transaction_date
                                                    or historical_prices.date = first_trade_date.first_trade_date)
                          where rolling_quantity < 0
                          order by expanded_transactions.security_id, expanded_transactions.account_id,
                                   historical_prices.date desc
                      )
             select null::int                                                                            as id,
                    'auto0_' || expanded_transactions_with_price.account_id || '_' ||
                    expanded_transactions_with_price.security_id                                         as uniq_id,
                    coalesce(ticker_options.last_price, expanded_transactions_with_price.adjusted_close) *
                    abs(rolling_quantity)                                                                as amount,
                    null::date                                                                           as date,
                    'ASSUMPTION BOUGHT ' || abs(rolling_quantity) || ' ' ||
                    coalesce(ticker_options.symbol || ' ' || to_char(ticker_options.expiration_date, 'MM/dd/YYYY') ||
                             ' ' ||
                             ticker_options.strike || ' ' || INITCAP(ticker_options.type),
                             expanded_transactions_with_price.name) ||
                    ' @ ' ||
                    coalesce(ticker_options.last_price, expanded_transactions_with_price.adjusted_close) as name,
                    coalesce(ticker_options.last_price, expanded_transactions_with_price.adjusted_close) as price,
                    abs(rolling_quantity)                                                                as quantity,
                    'buy'                                                                                as subtype,
                    'buy'                                                                                as type,
                    expanded_transactions_with_price.security_id,
                    expanded_transactions_with_price.profile_id,
                    expanded_transactions_with_price.account_id,
                    abs(rolling_quantity)                                                                as quantity_norm
             from expanded_transactions_with_price
                      left join {{ ref('portfolio_securities_normalized') }}
                                on portfolio_securities_normalized.id = expanded_transactions_with_price.security_id
                      left join {{ ref('ticker_options') }}
                                on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
             where rolling_quantity < 0
               and (ticker_options.contract_name is not null or
                    expanded_transactions_with_price.adjusted_close is not null)
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

select t.id,
       t.uniq_id::varchar,
       t.amount,
       t.date,
       t.name,
       t.price,
       t.quantity / case
                        when robinhood_options.quantity_module_sum = 0 and
                             portfolio_securities_normalized.type = 'derivative' and
                             plaid_institutions.ref_id = 'ins_54' then 100
                        else 1 end as quantity,
       t.subtype,
       t.type,
       t.security_id,
       t.profile_id,
       t.account_id,
       t.quantity_norm / case
                             when robinhood_options.quantity_module_sum = 0 and
                                  portfolio_securities_normalized.type = 'derivative' and
                                  plaid_institutions.ref_id = 'ins_54' then 100
                             else 1 end as quantity_norm,
       t.updated_at
from (
         select *,
                now() as updated_at
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
                   null::date                                                                   as date,
                   'ASSUMPTION BOUGHT ' || diff || ' ' ||
                   coalesce(ticker_options.name, t.name) || ' @ ' ||
                   coalesce(ticker_options.last_price, historical_prices.adjusted_close)        as name,
                   coalesce(ticker_options.last_price, historical_prices.adjusted_close)        as price,
                   diff                                                                         as quantity,
                   'buy'                                                                        as subtype,
                   'buy'                                                                        as type,
                   security_id,
                   profile_id,
                   account_id,
                   diff                                                                         as quantity_norm,
                   now() as updated_at
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
               and portfolio_securities_normalized.type != 'cash'
         )

         union all

         select id,
                id || '_' || account_id || '_' || security_id as uniq_id,
                amount,
                date                                          as date,
                name,
                price,
                quantity,
                subtype,
                type,
                security_id,
                profile_id,
                account_id,
                quantity_norm,
                now() as updated_at
         from normalized_transactions
     ) t
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = t.security_id
         join {{ ref('profile_holdings_normalized') }}
              on profile_holdings_normalized.profile_id = t.profile_id
                  and profile_holdings_normalized.security_id = t.security_id
                  and profile_holdings_normalized.account_id = t.account_id
         left join {{ source('app', 'profile_portfolio_accounts') }} on profile_portfolio_accounts.id = t.account_id
         left join {{ source('app', 'profile_plaid_access_tokens') }} on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id
         left join {{ source('app', 'plaid_institutions') }} on plaid_institutions.id = profile_plaid_access_tokens.institution_id
         left join robinhood_options on robinhood_options.profile_id = t.profile_id
