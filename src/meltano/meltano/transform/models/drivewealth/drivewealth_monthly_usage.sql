{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with usage as
         (
             select profile_id,
                    equity_value,
                    drivewealth_accounts_positions.created_at::date as date
             from {{ source('app', 'drivewealth_accounts_positions') }}
                      join {{ source('app', 'drivewealth_accounts') }}
                           on drivewealth_accounts.ref_id = drivewealth_accounts_positions.drivewealth_account_id
                      join {{ source('app', 'drivewealth_users') }}
                           on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
         ),
     profiles as
         (
             select profile_id, min(date_trunc('month', date)) as first_period_datetime
             from usage
             group by profile_id
         ),
     daily_usage as
         (
             select profile_id,
                    max(equity_value) as equity_value,
                    date
             from usage
             group by profile_id, date
     ),
     daily_usage_expanded as
         (

             select profile_id,
                    date,
                    coalesce(
                            equity_value,
                            LAST_VALUE_IGNORENULLS(equity_value)
                            over (partition by profile_id order by date rows between unbounded preceding and current row),
                            0
                        )                                                                  as equity_value,
                    date_trunc('month', date)                                              as period_start,
                    (date_trunc('month', date + interval '1 month') - interval '1 second') as period_end
             from profiles
                      join generate_series(first_period_datetime, now(), interval '1 day') date on true
                      left join daily_usage using (profile_id, date)
     )
select profile_id,
       period_start,
       period_end,
       avg(equity_value) as equity_value
from daily_usage_expanded
group by profile_id, period_start, period_end
