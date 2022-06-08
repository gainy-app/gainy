{{
  config(
    materialized = "incremental",
    unique_key = "uniq_id",
    tags = ["realtime"],
    post_hook=[
      index(this, 'uniq_id', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
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
                    type,
                    security_id,
                    profile_id,
                    account_id,
                    abs(quantity) * case when type = 'sell' then -1 else 1 end as quantity_norm
             from {{ source('app', 'profile_portfolio_transactions') }}
         ),
     first_trade_date as ( select code, min(date) as first_trade_date from {{ ref('historical_prices') }} group by code ),
     first_transaction_date as
         (
             select profile_id,
                    min(date) as profile_first_transaction_date
             from normalized_transactions
             group by profile_id
         ),
     mismatched_sell_transactions as
         (
             with expanded_transactions0 as
                      (
                          select *,
                                 sum(quantity_norm)
                                 over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as rolling_quantity
                          from normalized_transactions
                      ),
                  expanded_transactions as
                      (
                          select security_id,
                                 account_id,
                                 min(profile_id)                     as profile_id,
                                 min(name)                           as name,
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
                                historical_prices.date,
                                historical_prices.adjusted_close
                          from expanded_transactions
                                   left join {{ ref('portfolio_securities_normalized') }}
                                             on portfolio_securities_normalized.id = expanded_transactions.security_id
                                   left join first_trade_date
                                             on first_trade_date.code = portfolio_securities_normalized.original_ticker_symbol
                                   left join first_transaction_date using (profile_id)
                                   left join {{ ref('historical_prices') }}
                                             on historical_prices.code = portfolio_securities_normalized.original_ticker_symbol
                                                 and (historical_prices.date between first_transaction_date.profile_first_transaction_date - interval '1 week' and first_transaction_date.profile_first_transaction_date
                                                    or historical_prices.date = first_trade_date.first_trade_date)
                          where rolling_quantity < 0
                          order by expanded_transactions.security_id, expanded_transactions.account_id,
                                   historical_prices.date desc
                      )
             select null::int                                                               as id,
                    'auto0_' || expanded_transactions_with_price.account_id || '_' ||
                    expanded_transactions_with_price.security_id                            as uniq_id,
                    expanded_transactions_with_price.adjusted_close * abs(rolling_quantity) as amount,
                    expanded_transactions_with_price.date::date                             as date,
                    expanded_transactions_with_price.adjusted_close                         as price,
                    'buy'                                                                   as type,
                    expanded_transactions_with_price.security_id,
                    expanded_transactions_with_price.profile_id,
                    expanded_transactions_with_price.account_id,
                    abs(rolling_quantity)                                                   as quantity_norm
             from expanded_transactions_with_price
             where rolling_quantity < 0
               and expanded_transactions_with_price.adjusted_close is not null
         ),
     mismatched_buy_transactions as
         (
             -- union all buy transactions
             with expanded_transactions0 as
                      (
                          select *,
                                 sum(quantity_norm)
                                 over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as rolling_quantity,
                                 row_number()
                                 over (partition by account_id, security_id order by date, type)                                                  as row_number
                          from (select id,
                                       amount,
                                       date,
                                       price,
                                       type,
                                       security_id,
                                       profile_id,
                                       account_id,
                                       quantity_norm
                                from normalized_transactions
                                union all
                                select id,
                                       amount,
                                       date,
                                       price,
                                       type,
                                       security_id,
                                       profile_id,
                                       account_id,
                                       quantity_norm
                                from mismatched_sell_transactions) t
                      ),
                  total_amount_to_sell as
                      (
                          select distinct on (
                              security_id, account_id, profile_id
                              ) security_id,
                                account_id,
                                profile_id,
                                rolling_quantity - profile_holdings_normalized.quantity as quantity
                          from expanded_transactions0
                                   left join {{ ref('profile_holdings_normalized') }} using (security_id, account_id, profile_id)
                          order by security_id, account_id, profile_id, row_number desc
                      ),
                  -- match each buy transaction with appropriate sell transaction
                  -- so that total sum equals to holding quantity
                  expanded_transactions as
                      (
                          select normalized_transactions.id,
                                 date,
                                 price,
                                 least(
                                         quantity_norm,
                                         t.rolling_quantity,
                                         total_amount_to_sell.quantity -
                                         coalesce(sum(least(quantity_norm,
                                                            t.rolling_quantity))
                                                  over (partition by security_id, account_id order by date rows between unbounded preceding and 1 preceding),
                                                  0)
                                     ) as sell_quantity,
                                 original_ticker_symbol,
                                 security_id,
                                 profile_id,
                                 account_id
                          from normalized_transactions
                                   join (
                              select distinct on (
                                  security_id,
                                  account_id
                                  ) security_id,
                                    account_id,
                                    rolling_quantity
                              from expanded_transactions0
                              order by security_id, account_id, date desc, type desc, rolling_quantity
                          ) t using (security_id, account_id)
                                   join total_amount_to_sell using (security_id, account_id, profile_id)
                                   join {{ ref('portfolio_securities_normalized') }}
                                        on portfolio_securities_normalized.id =
                                           normalized_transactions.security_id
                          where normalized_transactions.type = 'buy'
                      )
             select null::int                                             as id,
                    'auto2_' || expanded_transactions.id                  as uniq_id,
                    historical_prices.adjusted_close * abs(sell_quantity) as amount,
                    expanded_transactions.date::date                      as date,
                    historical_prices.adjusted_close                      as price,
                    'sell'                                                as type,
                    expanded_transactions.security_id,
                    expanded_transactions.profile_id,
                    expanded_transactions.account_id,
                    -abs(sell_quantity)                                   as quantity_norm
             from expanded_transactions
                      join {{ ref('historical_prices') }}
                           on historical_prices.code = expanded_transactions.original_ticker_symbol
                               and historical_prices.date = expanded_transactions.date
             where sell_quantity > 0
               and historical_prices.adjusted_close is not null
         ),
     expanded_transactions as
         (
             select *,
                    sum(quantity_norm)
                    over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as rolling_quantity,
                    row_number()
                    over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as row_num
             from (
                      select account_id, security_id, date, quantity_norm, type, profile_id
                      from normalized_transactions
                      union all
                      select account_id, security_id, null as date, quantity_norm, type, profile_id
                      from mismatched_sell_transactions
                  ) t
         )

select sum(quantity_norm) * sum(abs(quantity_norm) * price) /
       sum(abs(quantity_norm))                                   as amount,
       sum(abs(quantity_norm) * price) / sum(abs(quantity_norm)) as price,
       sum(quantity_norm / case
                               when robinhood_options.quantity_module_sum = 0 and
                                    portfolio_securities_normalized.type = 'derivative' and
                                    plaid_institutions.ref_id = 'ins_54' then 100
                               else 1 end)                       as quantity,
       t.security_id,
       t.profile_id,
       t.account_id,
       sum(quantity_norm / case
                               when robinhood_options.quantity_module_sum = 0 and
                                    portfolio_securities_normalized.type = 'derivative' and
                                    plaid_institutions.ref_id = 'ins_54' then 100
                               else 1 end)                       as quantity_norm,
       sum(quantity_norm / case
                               when robinhood_options.quantity_module_sum = 0 and
                                    portfolio_securities_normalized.type = 'derivative' and
                                    plaid_institutions.ref_id = 'ins_54' then 100
                               else 1
           end * case
                     when portfolio_securities_normalized.type = 'derivative'
                         then 100
                     else 1 end)                                 as quantity_norm_for_valuation, -- to multiple by price
       now()                                                     as updated_at,
       max(uniq_id)::varchar                                     as uniq_id,
       t.date
from (
         select *
         from mismatched_sell_transactions

         union all

         select *
         from mismatched_buy_transactions

         union all

         -- mismatched holdings
         (
             select distinct on (
                 security_id,
                 account_id
                 ) null::int                                    as id,
                   'auto1_' || account_id || '_' || security_id as uniq_id,
                   historical_prices.adjusted_close * diff      as amount,
                   historical_prices.date::date                 as date,
                   historical_prices.adjusted_close             as price,
                   'buy'                                        as type,
                   security_id,
                   profile_id,
                   account_id,
                   diff                                         as quantity_norm
             from (
                      select distinct on (
                          profile_holdings_normalized.account_id, profile_holdings_normalized.security_id
                          ) profile_holdings_normalized.quantity,
                            profile_holdings_normalized.name,
                            portfolio_securities_normalized.original_ticker_symbol,
                            profile_holdings_normalized.security_id,
                            profile_holdings_normalized.profile_id,
                            profile_holdings_normalized.account_id,
                            profile_first_transaction_date,
                            profile_holdings_normalized.quantity -
                            coalesce(expanded_transactions.rolling_quantity, 0) as diff
                      from {{ ref('profile_holdings_normalized') }}
                               join {{ ref('portfolio_securities_normalized') }}
                                    on portfolio_securities_normalized.id = profile_holdings_normalized.security_id
                               left join first_transaction_date using (profile_id)
                               left join expanded_transactions
                                         on profile_holdings_normalized.account_id = expanded_transactions.account_id and
                                            profile_holdings_normalized.security_id = expanded_transactions.security_id
                      where portfolio_securities_normalized.type != 'cash'
                      order by profile_holdings_normalized.account_id, profile_holdings_normalized.security_id,
                          expanded_transactions.row_num desc
                  ) t
                      left join first_trade_date on first_trade_date.code = original_ticker_symbol
                      left join {{ ref('historical_prices') }}
                                on historical_prices.code = original_ticker_symbol and
                                   (historical_prices.date between profile_first_transaction_date - interval '1 week' and profile_first_transaction_date or
                                    historical_prices.date = first_trade_date.first_trade_date)
             where diff > 0
             order by security_id, account_id, historical_prices.date desc
         )

         union all

         select id,
                id || '_' || account_id || '_' || security_id as uniq_id,
                amount,
                date,
                price,
                type,
                security_id,
                profile_id,
                account_id,
                quantity_norm
         from normalized_transactions
     ) t
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = t.security_id
         left join {{ source('app', 'profile_portfolio_accounts') }} on profile_portfolio_accounts.id = t.account_id
         left join {{ source('app', 'profile_plaid_access_tokens') }} on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id
         left join {{ source('app', 'plaid_institutions') }} on plaid_institutions.id = profile_plaid_access_tokens.institution_id
         left join robinhood_options on robinhood_options.profile_id = t.profile_id
where t.type in ('buy', 'sell')
group by t.date, t.security_id, t.account_id, t.profile_id
having sum(abs(quantity_norm)) > 0
