{{
  config(
    materialized = "incremental",
    unique_key = "transaction_uniq_id",
    tags = ["realtime"],
    post_hook=[
      index('transaction_uniq_id', true),
      'create index if not exists pet_profile_id_symbol_datetime_index on {{this}} (profile_id, symbol, datetime)',
      'delete from {{this}}
        using (select profile_id, max(updated_at) as max_updated_at from {{this}} group by profile_id) old_stats
        where old_stats.profile_id = {{this}}.profile_id
        and {{this}}.updated_at < old_stats.max_updated_at'
    ]
  )
}}


with plaid_transactions as
         (
             select profile_portfolio_transactions.id,
                    profile_portfolio_transactions.id || '_' || account_id || '_' || security_id as uniq_id,
                    portfolio_securities_normalized.original_ticker_symbol                       as symbol,
                    portfolio_securities_normalized.type                                         as security_type,
                    security_id,
                    account_id,
                    profile_portfolio_transactions.amount,
                    profile_portfolio_transactions.date,
                    profile_portfolio_transactions.name,
                    profile_portfolio_transactions.price,
                    profile_portfolio_transactions.type,
                    profile_id,
                    profile_holdings_normalized_all.holding_id_v2,
                    (case
                         when profile_portfolio_transactions.type = 'sell'
                             then -1
                         else 1
                         end * abs(profile_portfolio_transactions.quantity))                     as quantity_norm
             from {{ source('app', 'profile_portfolio_transactions') }}
                      left join {{ ref('profile_holdings_normalized_all') }} using (profile_id, security_id, account_id)
                      left join {{ ref('portfolio_securities_normalized') }} on portfolio_securities_normalized.id = profile_portfolio_transactions.security_id
         ),
     first_trade_date as (select symbol, date_all as first_trade_date from {{ ref('historical_prices_marked') }}),
     first_transaction_date as
         (
             select profile_id,
                    min(date) as profile_first_transaction_date
             from app.profile_portfolio_transactions
             group by profile_id
     ),
     mismatched_sell_transactions as
         (
             with expanded_transactions0 as
                      (
                          select *,
                                 sum(quantity_norm)
                                 over (partition by account_id, security_id order by date, type rows between unbounded preceding and current row) as rolling_quantity
                          from plaid_transactions
                      ),
                  expanded_transactions as
                      (
                          select security_id,
                                 account_id,
                                 holding_id_v2,
                                 symbol,
                                 min(security_type)    as security_type,
                                 min(profile_id)       as profile_id,
                                 min(name)             as name,
                                 min(rolling_quantity) as rolling_quantity
                          from expanded_transactions0
                          group by security_id, account_id, holding_id_v2, symbol
                  ),
                  expanded_transactions_with_price as
                      (
                          select distinct on (
                              expanded_transactions.security_id,
                              expanded_transactions.account_id
                              ) expanded_transactions.security_id,
                                expanded_transactions.account_id,
                                expanded_transactions.profile_id,
                                expanded_transactions.holding_id_v2,
                                expanded_transactions.symbol,
                                expanded_transactions.name,
                                expanded_transactions.security_type,
                                expanded_transactions.rolling_quantity,
                                historical_prices.date,
                                historical_prices.adjusted_close
                          from expanded_transactions
                                   left join first_transaction_date using (profile_id)
                                   left join first_trade_date using (symbol)
                                   left join {{ ref('historical_prices') }}
                                             on historical_prices.symbol = expanded_transactions.symbol
                                                 and (historical_prices.date between first_transaction_date.profile_first_transaction_date - interval '1 week' and first_transaction_date.profile_first_transaction_date
                                                    or historical_prices.date = first_trade_date.first_trade_date)
                          where rolling_quantity < 0
                          order by expanded_transactions.security_id, expanded_transactions.account_id,
                                   historical_prices.date desc
                  )
             select null::int                                                               as id,
                    'auto0_' || expanded_transactions_with_price.account_id || '_' ||
                    expanded_transactions_with_price.security_id                            as uniq_id,
                    holding_id_v2,
                    symbol,
                    expanded_transactions_with_price.adjusted_close * abs(rolling_quantity) as amount,
                    expanded_transactions_with_price.date::date                             as date,
                    expanded_transactions_with_price.adjusted_close                         as price,
                    'buy'                                                                   as type,
                    expanded_transactions_with_price.security_id,
                    expanded_transactions_with_price.profile_id,
                    expanded_transactions_with_price.account_id,
                    expanded_transactions_with_price.security_type,
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
                          from (
                                   select id,
                                          amount,
                                          date,
                                          price,
                                          type,
                                          security_id,
                                          profile_id,
                                          account_id,
                                          quantity_norm
                                   from plaid_transactions
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
                                   from mismatched_sell_transactions
                               ) t
                      ),
                  total_amount_to_sell as
                      (
                          select distinct on (
                              security_id, account_id, profile_id
                              ) security_id,
                                account_id,
                                profile_id,
                                rolling_quantity - profile_holdings_normalized_all.quantity as quantity
                          from expanded_transactions0
                                   left join {{ ref('profile_holdings_normalized_all') }} using (security_id, account_id, profile_id)
                          order by security_id, account_id, profile_id, row_number desc
                  ),
                  -- match each buy transaction with appropriate sell transaction
                  -- so that total sum equals to holding quantity
                  expanded_transactions as
                      (
                          select plaid_transactions.id,
                                 holding_id_v2,
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
                                 symbol,
                                 security_id,
                                 profile_id,
                                 account_id,
                                 security_type
                          from plaid_transactions
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
                          where plaid_transactions.type = 'buy'
                  )
             select null::int                                             as id,
                    'auto2_' || expanded_transactions.id                  as uniq_id,
                    holding_id_v2,
                    symbol,
                    historical_prices.adjusted_close * abs(sell_quantity) as amount,
                    expanded_transactions.date::date                      as date,
                    historical_prices.adjusted_close                      as price,
                    'sell'                                                as type,
                    expanded_transactions.security_id,
                    expanded_transactions.profile_id,
                    expanded_transactions.account_id,
                    expanded_transactions.security_type,
                    -abs(sell_quantity)                                   as quantity_norm
             from expanded_transactions
                      join {{ ref('historical_prices') }} using(symbol, date)
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
                      from plaid_transactions
                      union all
                      select account_id, security_id, null as date, quantity_norm, type, profile_id
                      from mismatched_sell_transactions
                  ) t
     ),
     mismatched_holdings_transactions as
         (
             select distinct on (
                 security_id,
                 account_id
                 ) null::int                                    as id,
                   'auto1_' || account_id || '_' || security_id as uniq_id,
                   holding_id_v2,
                   t.symbol,
                   historical_prices.adjusted_close * diff      as amount,
                   historical_prices.date::date                 as date,
                   historical_prices.adjusted_close             as price,
                   'buy'                                        as type,
                   security_id,
                   profile_id,
                   account_id,
                   security_type,
                   diff                                         as quantity_norm
             from (
                      select distinct on (
                          profile_holdings_normalized.account_id, profile_holdings_normalized.security_id
                          ) profile_holdings_normalized.quantity,
                            profile_holdings_normalized.name,
                            profile_holdings_normalized.holding_id_v2,
                            profile_holdings_normalized.symbol,
                            profile_holdings_normalized.security_id,
                            profile_holdings_normalized.profile_id,
                            profile_holdings_normalized.account_id,
                            profile_holdings_normalized.type                                                           as security_type,
                            profile_first_transaction_date,
                            profile_holdings_normalized.quantity - coalesce(expanded_transactions.rolling_quantity, 0) as diff
                      from {{ ref('profile_holdings_normalized') }}
                               left join first_transaction_date using (profile_id)
                               left join expanded_transactions using (account_id, security_id)
                      where profile_holdings_normalized.type != 'cash'
                        and not profile_holdings_normalized.is_app_trading
                      order by profile_holdings_normalized.account_id, profile_holdings_normalized.security_id,
                               expanded_transactions.row_num desc
                  ) t
                      left join first_trade_date using (symbol)
                      left join {{ ref('historical_prices') }}
                                on historical_prices.symbol = t.symbol
                                   and (historical_prices.date between profile_first_transaction_date - interval '1 week'
                                        and profile_first_transaction_date
                                            or historical_prices.date = first_trade_date.first_trade_date)
             where diff > 0
             order by security_id, account_id, historical_prices.date desc
     ),
     dw_transactions as
         (
             with stats as
                      (
                          select holding_id_v2, min(date) as min_date
                          from {{ ref('drivewealth_portfolio_historical_holdings') }}
                          group by holding_id_v2
                      )
             select holding_id_v2                        as uniq_id,
                    symbol,
                    profile_holdings_normalized_all.holding_id_v2,
                    null::double precision               as quantity_norm,
                    null::double precision               as price,
                    min_date                             as datetime,
                    'buy'                                as type,
                    null::int                            as security_id,
                    profile_id,
                    null::int                            as account_id,
                    profile_holdings_normalized_all.type as security_type,
                    collection_id
             from stats
                      left join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
     ),
     groupped_expanded_transactions as
         (
             select t.symbol,
                    max(coalesce(ticker_options.symbol, base_tickers.symbol)) as ticker_symbol,
                    sum(quantity_norm) * sum(abs(quantity_norm) * price) /
                    sum(abs(quantity_norm))                                   as amount,
                    sum(abs(quantity_norm) * price) / sum(abs(quantity_norm)) as price,
                    t.holding_id_v2,
                    t.profile_id,
                    sum(quantity_norm)                                        as quantity_norm,
                    sum(quantity_norm * case
                                            when t.security_type = 'derivative'
                                                then 100
                                            else 1 end)                       as quantity_norm_for_valuation, -- to multiple by price
                    max(uniq_id)::varchar                                     as transaction_uniq_id,
                    security_type,
                    is_app_trading,
                    t.datetime
             from (
                      select id,
                             uniq_id,
                             symbol,
                             amount,
                             date::timestamp as datetime,
                             price,
                             type,
                             security_type,
                             profile_id,
                             holding_id_v2,
                             null::int       as collection_id,
                             quantity_norm::double precision,
                             false           as is_app_trading
                      from mismatched_sell_transactions

                      union all

                      select id,
                             uniq_id,
                             symbol,
                             amount,
                             date::timestamp as datetime,
                             price,
                             type,
                             security_type,
                             profile_id,
                             holding_id_v2,
                             null::int       as collection_id,
                             quantity_norm::double precision,
                             false           as is_app_trading
                      from mismatched_buy_transactions

                      union all

                      select id,
                             uniq_id,
                             symbol,
                             amount,
                             date::timestamp as datetime,
                             price,
                             type,
                             security_type,
                             profile_id,
                             holding_id_v2,
                             null::int       as collection_id,
                             quantity_norm::double precision,
                             false           as is_app_trading
                      from mismatched_holdings_transactions

                      union all

                      select id,
                             uniq_id,
                             symbol,
                             amount,
                             date::timestamp as datetime,
                             price,
                             type,
                             security_type,
                             profile_id,
                             holding_id_v2,
                             null::int       as collection_id,
                             quantity_norm::double precision,
                             false           as is_app_trading
                      from plaid_transactions

                      union all

                      select null                   as id,
                             uniq_id,
                             symbol,
                             null::double precision as amount,
                             datetime::timestamp,
                             price,
                             type,
                             security_type,
                             profile_id,
                             holding_id_v2,
                             collection_id,
                             quantity_norm::double precision,
                             true                   as is_app_trading
                      from dw_transactions
                  ) t
                      left join {{ ref('base_tickers') }} using (symbol)
                      left join {{ ref('ticker_options') }} on ticker_options.contract_name = t.symbol
             where t.type in ('buy', 'sell')
               and (base_tickers.symbol is not null or ticker_options.symbol is not null)
             group by t.holding_id_v2, t.datetime, t.profile_id, t.symbol, t.security_type, t.is_app_trading
             having sum(abs(quantity_norm)) > 0 or is_app_trading
{% if is_incremental() %}
     ),
     profiles_to_update as
         (
             select distinct groupped_expanded_transactions.profile_id
             from groupped_expanded_transactions
                      left join {{ this }} old_data using (transaction_uniq_id)
             where old_data.transaction_uniq_id is null
                or old_data.quantity_norm != groupped_expanded_transactions.quantity_norm
                or old_data.datetime is null and groupped_expanded_transactions.datetime is not null
                or old_data.datetime is not null and groupped_expanded_transactions.datetime is null
                or (old_data.datetime is not null and groupped_expanded_transactions.datetime is not null and old_data.datetime != groupped_expanded_transactions.datetime)
{% endif %}
     )
select groupped_expanded_transactions.symbol,
       groupped_expanded_transactions.ticker_symbol,
       groupped_expanded_transactions.amount,
       groupped_expanded_transactions.price,
       groupped_expanded_transactions.quantity_norm as quantity,
       groupped_expanded_transactions.holding_id_v2,
       groupped_expanded_transactions.profile_id,
       groupped_expanded_transactions.quantity_norm,
       groupped_expanded_transactions.quantity_norm_for_valuation,
       groupped_expanded_transactions.transaction_uniq_id::text,
       groupped_expanded_transactions.datetime,
       groupped_expanded_transactions.security_type,
       groupped_expanded_transactions.is_app_trading,
       now() as updated_at
from groupped_expanded_transactions
{% if is_incremental() %}
         join profiles_to_update using (profile_id)
{% endif %}
