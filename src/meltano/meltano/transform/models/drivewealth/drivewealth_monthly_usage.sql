{{
  config(
    materialized = "view",
  )
}}


with usage as
         (
             select profile_id, date, sum(value) as value
             from (
                      (
                          select distinct on (
                              drivewealth_account_id,
                              (created_at at time zone 'America/New_York')::date
                              ) drivewealth_account_id,
                                equity_value                                       as value,
                                (created_at at time zone 'America/New_York')::date as date
                          from {{ source('app', 'drivewealth_accounts_positions') }}
                          order by drivewealth_account_id desc,
                                   (created_at at time zone 'America/New_York')::date desc,
                                   created_at desc
                      )

                      union all

                      (
                          select distinct on (
                              drivewealth_account_id,
                              (created_at at time zone 'America/New_York')::date
                              ) drivewealth_account_id,
                                cash_balance                                       as value,
                                (created_at at time zone 'America/New_York')::date as date
                          from {{ source('app', 'drivewealth_accounts_money') }}
                          order by drivewealth_account_id desc,
                                   (created_at at time zone 'America/New_York')::date desc,
                                   created_at desc
                      )
                  ) t
                      join {{ source('app', 'drivewealth_accounts') }}
                           on drivewealth_accounts.ref_id = t.drivewealth_account_id
                      join {{ source('app', 'drivewealth_users') }}
                           on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
             group by profile_id, date
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
                    max(value) as value,
                    date
             from usage
             group by profile_id, date
     ),
     daily_usage_expanded as
         (

             select profile_id,
                    date,
                    coalesce(
                            value,
                            LAST_VALUE_IGNORENULLS(value)
                            over (partition by profile_id order by date rows between unbounded preceding and current row),
                            0
                        )                                                                  as value,
                    date_trunc('month', date)                                              as period_start,
                    date_trunc('month', date + interval '1 month') as period_end
             from profiles
                      join generate_series(first_period_datetime, now(), interval '1 day') date on true
                      left join daily_usage using (profile_id, date)
     )
select profile_id,
       period_start,
       period_end,
       avg(value) as value
from daily_usage_expanded
group by profile_id, period_start, period_end
having sum(value) > 0
