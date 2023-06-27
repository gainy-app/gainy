{{
  config(
    materialized = "view",
  )
}}


with profile_scoring_settings_stats as
         (
             select (created_at at time zone 'America/New_York')::date as date,
                    count(*)                                           as questionaire_completed_cnt
             from {{ source('app', 'profile_scoring_settings') }}
             group by (created_at at time zone 'America/New_York')::date
         ),
     kyc_stats as
         (
             select (created_at at time zone 'America/New_York')::date                      as date,
                    sum((status = 'APPROVED')::int)                                         as is_kyc_passed,
                    sum((status is not null and status not in ('APPROVED', 'DENIED'))::int) as is_kyc_pending
             from (
                      select distinct on (profile_id) * from {{ source('app', 'kyc_statuses') }} order by profile_id, created_at desc
                  ) t
             group by (created_at at time zone 'America/New_York')::date
     ),
     deposit_stats as
         (
             select (created_at at time zone 'America/New_York')::date as date,
                    sum(amount)                                        as total_deposit_amount
             from {{ source('app', 'trading_money_flow') }}
             where amount > 0
               and status != 'FAILED'
               and type = 'MANUAL'
             group by (created_at at time zone 'America/New_York')::date
     ),
     aum_stats as
         (
             select date, sum(value) as aum_amount
             from (
                      (
                          select distinct on (
                              drivewealth_account_id,
                              (created_at at time zone 'America/New_York')::date
                              ) equity_value                                       as value,
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
                              ) cash_balance                                       as value,
                                (created_at at time zone 'America/New_York')::date as date
                          from {{ source('app', 'drivewealth_accounts_money') }}
                          order by drivewealth_account_id desc,
                                   (created_at at time zone 'America/New_York')::date desc,
                                   created_at desc
                      )
                  ) t
             group by date
     )
select date,
       coalesce(questionaire_completed_cnt, 0) as questionaire_completed_cnt,
       coalesce(is_kyc_passed, 0)              as is_kyc_passed,
       coalesce(is_kyc_pending, 0)             as is_kyc_pending,
       coalesce(total_deposit_amount, 0)       as total_deposit_amount,
       coalesce(aum_amount, 0)                 as aum_amount
from (
         select generate_series(min_date, (now() at time zone 'America/New_York' - interval '1 day')::date,
                                interval '1 day')::date as date
         from (
                  select min(min_date) as min_date
                  from (
                           select min(date) as min_date
                           from profile_scoring_settings_stats
                           union
                           select min(date) as min_date
                           from kyc_stats
                           union
                           select min(date) as min_date
                           from deposit_stats
                           union
                           select min(date) as min_date
                           from aum_stats
                       ) t
              ) t
     ) dates
         left join profile_scoring_settings_stats using (date)
         left join kyc_stats using (date)
         left join deposit_stats using (date)
         left join aum_stats using (date)