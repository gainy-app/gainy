{{
  config(
    materialized = "incremental",
    tags = ["realtime"],
    unique_key = "id",
    post_hook=[
      pk('id'),
      index('symbol'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = {{this}}.period
        and {{this}}.updated_at < dc_stats.max_updated_at',
      'delete from {{this}} where id = \'fake_row_allowing_deletion\'',
    ]
  )
}}


select distinct on (
        collection_ticker_actual_weights.symbol
    ) 'ttf_ticker_missing_in_drivewealth_instruments_' || collection_ticker_actual_weights.symbol as id,
       collection_ticker_actual_weights.symbol,
       'ttf_ticker_missing_in_drivewealth_instruments'                                             as code,
       'daily'                                                                                     as period,
       'TTF ticker ' || collection_ticker_actual_weights.symbol ||
           ' is missing in app.drivewealth_instruments'                                            as message,
       now()                                                                                       as updated_at
from {{ ref('collection_ticker_actual_weights') }}
         left join {{ source('app', 'drivewealth_instruments') }}
                   on collection_ticker_actual_weights.symbol = normalize_drivewealth_symbol(drivewealth_instruments.symbol)
where drivewealth_instruments.symbol is null
   or drivewealth_instruments.status != 'ACTIVE'

union all

(
    with transaction_stats as
             (
                 select profile_id, sum(account_amount_delta) as transactions_sum
                 from {{ source('app', 'drivewealth_transactions') }}
                          join {{ source('app', 'drivewealth_accounts') }} on drivewealth_accounts.ref_id = account_id
                          join {{ source('app', 'drivewealth_users') }} on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
                 where profile_id is not null
                 group by profile_id
             ),
         cash_flow_stats as
             (
                 select profile_id, sum(cash_flow) cash_flow_sum
                 from {{ ref('drivewealth_portfolio_historical_holdings') }}
                 group by profile_id

         )
    select 'dw_cf_transactions_diff_' || profile_id           as id,
           null                                               as symbol,
           'dw_cf_transactions_diff'                          as code,
           'realtime'                                         as period,
           'Profile ' || profile_id || ' cash_flow differs from transaction sum: ' ||
           abs(transactions_sum - coalesce(cash_flow_sum, 0)) as message,
           now()                                              as updated_at
    from transaction_stats
             left join cash_flow_stats using (profile_id)
    where abs(transactions_sum - coalesce(cash_flow_sum, 0)) > 1
)

union all

(
    select 'trading_hanging_pending_transaction_' || order_id as id,
           null                                               as symbol,
           'trading_hanging_pending_transaction'              as code,
           'realtime'                                         as period,
           'Profile ' || profile_id ||
           ' has hanging pending order: ' || order_id         as message,
           now()                                              as updated_at
    from (
             select profile_id,
                    order_id,
                    sum(least(now(), close_at) - greatest(open_at, created_at)) as time_waiting
             from (
                      select 'tcv_' || id as order_id, profile_id, created_at, status
                      from {{ source('app', 'trading_collection_versions') }}
                      union all
                      select 'to_' || id as order_id, profile_id, created_at, status
                      from {{ source('app', 'trading_orders') }}
                  ) t
                      join {{ ref('exchange_schedule') }} on exchange_name = 'NYSE' and close_at > created_at and open_at < now()
             where status in ('PENDING', 'PENDING_EXECUTION')
             group by profile_id, order_id
         ) t
    where time_waiting > interval '1 hour'
)

union all

(
    with latest_portfolio_status as
             (
                 select distinct on (
                     profile_id
                     ) profile_id,
                       equity_value
                 from {{ source('app', 'drivewealth_portfolio_statuses') }}
                          join {{ source('app', 'drivewealth_portfolios') }} on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
                 order by profile_id, drivewealth_portfolio_statuses.created_at desc
             ),
         transaction_stats as
             (
                 select profile_id,
                        sum(account_amount_delta) as transactions_sum
                 from {{ source('app', 'drivewealth_transactions') }}
                          join {{ source('app', 'drivewealth_accounts') }} on drivewealth_accounts.ref_id = drivewealth_transactions.account_id
                          join {{ source('app', 'drivewealth_users') }} on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
                 group by profile_id
         )
    select 'dw_wrong_total_portfolio_gain_' || profile_id                                                    as id,
           null                                                                                              as symbol,
           'dw_wrong_total_portfolio_gain'                                                                   as code,
           'realtime'                                                                                        as period,
           'Profile ' || profile_id ||
           ' has wrong total portfolio gain, diff: ' ||
           abs(coalesce(transactions_sum, 0) + coalesce(absolute_gain_total, 0) - coalesce(equity_value, 0)) as message,
           now()                                                                                             as updated_at
    from {{ ref('drivewealth_portfolio_gains') }}
             left join latest_portfolio_status using (profile_id)
             left join transaction_stats using (profile_id)
    where profile_id > 1
      and abs(coalesce(transactions_sum, 0) + coalesce(absolute_gain_total, 0) - coalesce(equity_value, 0)) > 1
)

union all

-- add one fake record to allow post_hook above to clean other rows
select 'fake_row_allowing_deletion' as id,
       null                         as symbol,
       null                         as code,
       period,
       null                         as message,
       now()                        as updated_at
from (values('daily', 'realtime')) t(period)
