{{
  config(
    materialized = "incremental",
    tags = ["realtime"],
    post_hook=[
      index(['profile_id', 'updated_at'], false),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

with profile_stats as materialized
         (
             select t.*,
                    lag(value) over wnd as prev_value
             from (
                      select profile_id,
                             date,
                             sum(value)      as value,
                             sum(cash_flow)  as cash_flow
                      from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      group by profile_id, date
                  ) t
                 window wnd as (partition by profile_id order by date)
     ),
     profile_values_marked as
         (
             with raw_data_1d as
                      (
                          select profile_id,
                                 value      as actual_value,
                                 prev_value as prev_value_1d,
                                 cash_flow  as cash_flow_sum_1d
                          from (
                                   select profile_id,
                                          max(date) as date
                                   from profile_stats
                                   where date > now()::date - interval '1 week'
                                   group by profile_id
                               ) t
                                   join profile_stats using (profile_id, date)
                      ),
                  raw_data_1w as
                      (
                          select profile_id,
                                 prev_value    as prev_value_1w,
                                 cash_flow_sum as cash_flow_sum_1w
                          from (
                                   select profile_id,
                                          min(date)      as date,
                                          sum(cash_flow) as cash_flow_sum
                                   from profile_stats
                                   where date > now()::date - interval '1 week'
                                   group by profile_id
                               ) t
                                   join profile_stats using (profile_id, date)
                  ),
                  raw_data_1m as
                      (
                          select profile_id,
                                 prev_value    as prev_value_1m,
                                 cash_flow_sum as cash_flow_sum_1m
                          from (
                                   select profile_id,
                                          min(date)      as date,
                                          sum(cash_flow) as cash_flow_sum
                                   from profile_stats
                                   where date > now()::date - interval '1 month'
                                   group by profile_id
                               ) t
                                   join profile_stats using (profile_id, date)
                  ),
                  raw_data_3m as
                      (
                          select profile_id,
                                 prev_value    as prev_value_3m,
                                 cash_flow_sum as cash_flow_sum_3m
                          from (
                                   select profile_id,
                                          min(date)      as date,
                                          sum(cash_flow) as cash_flow_sum
                                   from profile_stats
                                   where date > now()::date - interval '3 month'
                                   group by profile_id
                               ) t
                                   join profile_stats using (profile_id, date)
                  ),
                  raw_data_1y as
                      (
                          select profile_id,
                                 prev_value    as prev_value_1y,
                                 cash_flow_sum as cash_flow_sum_1y
                          from (
                                   select profile_id,
                                          min(date)      as date,
                                          sum(cash_flow) as cash_flow_sum
                                   from profile_stats
                                   where date > now()::date - interval '1 year'
                                   group by profile_id
                               ) t
                                   join profile_stats using (profile_id, date)
                  ),
                  raw_data_5y as
                      (
                          select profile_id,
                                 prev_value    as prev_value_5y,
                                 cash_flow_sum as cash_flow_sum_5y
                          from (
                                   select profile_id,
                                          min(date)      as date,
                                          sum(cash_flow) as cash_flow_sum
                                   from profile_stats
                                   where date > now()::date - interval '5 years'
                                   group by profile_id
                               ) t
                                   join profile_stats using (profile_id, date)
                  ),
                  raw_data_all as
                      (
                          select profile_id,
                                 cash_flow_sum_total
                          from (
                                   select profile_id,
                                          min(date)      as date,
                                          sum(cash_flow) as cash_flow_sum_total
                                   from profile_stats
                                   group by profile_id
                               ) t
                                   join profile_stats using (profile_id, date)
                  )
             select profile_id,
                    actual_value,
                    prev_value_1d,
                    cash_flow_sum_1d,
                    prev_value_1w,
                    cash_flow_sum_1w,
                    prev_value_1m,
                    cash_flow_sum_1m,
                    prev_value_3m,
                    cash_flow_sum_3m,
                    prev_value_1y,
                    cash_flow_sum_1y,
                    prev_value_5y,
                    cash_flow_sum_5y,
                    cash_flow_sum_total
             from raw_data_all
                      left join raw_data_1d using (profile_id)
                      left join raw_data_1w using (profile_id)
                      left join raw_data_1m using (profile_id)
                      left join raw_data_3m using (profile_id)
                      left join raw_data_1y using (profile_id)
                      left join raw_data_5y using (profile_id)
     ),
     gains as
         (
             select profile_id,
                    actual_value,
                    actual_value - prev_value_1d - cash_flow_sum_1d as absolute_gain_1d,
                    actual_value - prev_value_1w - cash_flow_sum_1w as absolute_gain_1w,
                    actual_value - prev_value_1m - cash_flow_sum_1m as absolute_gain_1m,
                    actual_value - prev_value_3m - cash_flow_sum_3m as absolute_gain_3m,
                    actual_value - prev_value_1y - cash_flow_sum_1y as absolute_gain_1y,
                    actual_value - prev_value_5y - cash_flow_sum_5y as absolute_gain_5y,
                    actual_value - cash_flow_sum_total              as absolute_gain_total
             from profile_values_marked
     ),
     relative_gains as
         (
             with data as
                     (
                         select profile_id,
                                date,
                                sum(value)              as value,
                                sum(prev_value)::numeric         as prev_value,
                                sum(case when is_last_date.holding_id_v2 is null then 0 else value end -
                                    cash_flow)::numeric as cash_flow
                         from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                  left join (
                                                select holding_id_v2, max(date) as date
                                                from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                                group by holding_id_v2
                                            ) is_last_date using (holding_id_v2, date)
                                  left join {{ ref('drivewealth_portfolio_historical_holdings_marked') }} using (holding_id_v2)
                         where date > last_selloff_date
                            or last_selloff_date is null
                         group by profile_id, date
                     ),
                 data_1d as
                     (
                         select distinct on (
                             profile_id
                             ) profile_id,
                               case
                                   when prev_value + (value - cash_flow) > 0
                                       then value / (prev_value + (value - cash_flow)) - 1
                                   when prev_value > 0
                                       then (value - (value - cash_flow)) / prev_value - 1
                                   end as relative_gain_1d
                         from data
                         where date >= now()::date - interval '1 week'
                         order by profile_id desc, date desc
                 ),
                 data_1w as
                     (
                         select profile_id,
                                (1 + xirr(cf, d)) ^ (dates_cnt / 365.0) - 1 as relative_gain_1w
                         from (
                                  select profile_id,
                                         array_agg(cash_flow order by date) as cf,
                                         array_agg(date order by date)      as d,
                                         count(date)                        as dates_cnt
                                  from (
                                           select profile_id,
                                                  date,
                                                  cash_flow - case
                                                                  when row_number()
                                                                       over (partition by profile_id order by date) = 1
                                                                      then prev_value
                                                                  else 0 end as cash_flow
                                           from data
                                           where date >= now()::date - interval '1 week'
                                       ) t
                                  group by profile_id
                              ) t
                 ),
                 data_1m as
                     (
                         select profile_id,
                                (1 + xirr(cf, d)) ^ (dates_cnt / 365.0) - 1 as relative_gain_1m
                         from (
                                  select profile_id,
                                         array_agg(cash_flow order by date) as cf,
                                         array_agg(date order by date)      as d,
                                         count(date)                        as dates_cnt
                                  from (
                                           select profile_id,
                                                  date,
                                                  cash_flow - case
                                                                  when row_number()
                                                                       over (partition by profile_id order by date) = 1
                                                                      then prev_value
                                                                  else 0 end as cash_flow
                                           from data
                                           where date >= now()::date - interval '1 month'
                                       ) t
                                  group by profile_id
                              ) t
                 ),
                 data_3m as
                     (
                         select profile_id,
                                (1 + xirr(cf, d)) ^ (dates_cnt / 365.0) - 1 as relative_gain_3m
                         from (
                                  select profile_id,
                                         array_agg(cash_flow order by date) as cf,
                                         array_agg(date order by date)      as d,
                                         count(date)                        as dates_cnt
                                  from (
                                           select profile_id,
                                                  date,
                                                  cash_flow - case
                                                                  when row_number()
                                                                       over (partition by profile_id order by date) = 1
                                                                      then prev_value
                                                                  else 0 end as cash_flow
                                           from data
                                           where date >= now()::date - interval '3 month'
                                       ) t
                                  group by profile_id
                              ) t
                 ),
                 data_1y as
                     (
                         select profile_id,
                                (1 + xirr(cf, d)) ^ (dates_cnt / 365.0) - 1 as relative_gain_1y
                         from (
                                  select profile_id,
                                         array_agg(cash_flow order by date) as cf,
                                         array_agg(date order by date)      as d,
                                         count(date)                        as dates_cnt
                                  from (
                                           select profile_id,
                                                  date,
                                                  cash_flow - case
                                                                  when row_number()
                                                                       over (partition by profile_id order by date) = 1
                                                                      then prev_value
                                                                  else 0 end as cash_flow
                                           from data
                                           where date >= now()::date - interval '1 year'
                                       ) t
                                  group by profile_id
                              ) t
                 ),
                 data_5y as
                     (
                         select profile_id,
                                (1 + xirr(cf, d)) ^ (dates_cnt / 365.0) - 1 as relative_gain_5y
                         from (
                                  select profile_id,
                                         array_agg(cash_flow order by date) as cf,
                                         array_agg(date order by date)      as d,
                                         count(date)                        as dates_cnt
                                  from (
                                           select profile_id,
                                                  date,
                                                  cash_flow - case
                                                                  when row_number()
                                                                       over (partition by profile_id order by date) = 1
                                                                      then prev_value
                                                                  else 0 end as cash_flow
                                           from data
                                           where date >= now()::date - interval '5 year'
                                       ) t
                                  group by profile_id
                              ) t
                 ),
                 data_total as
                     (
                         select profile_id,
                                (1 + xirr(cf, d)) ^ (dates_cnt / 365.0) - 1 as relative_gain_total
                         from (
                                  select profile_id,
                                         array_agg(cash_flow order by date) as cf,
                                         array_agg(date order by date)      as d,
                                         count(date)                        as dates_cnt
                                  from data
                                  group by profile_id
                              ) t
                 )
             select profile_id,
                    relative_gain_1d,
                    relative_gain_1w,
                    relative_gain_1m,
                    relative_gain_3m,
                    relative_gain_1y,
                    relative_gain_5y,
                    relative_gain_total
             from data_total
                      left join data_1d using (profile_id)
                      left join data_1w using (profile_id)
                      left join data_1m using (profile_id)
                      left join data_3m using (profile_id)
                      left join data_1y using (profile_id)
                      left join data_5y using (profile_id)
     )
select profile_id,
       actual_value,
       coalesce(absolute_gain_1d, absolute_gain_total) as absolute_gain_1d,
       coalesce(absolute_gain_1w, absolute_gain_total) as absolute_gain_1w,
       coalesce(absolute_gain_1m, absolute_gain_total) as absolute_gain_1m,
       coalesce(absolute_gain_3m, absolute_gain_total) as absolute_gain_3m,
       coalesce(absolute_gain_1y, absolute_gain_total) as absolute_gain_1y,
       coalesce(absolute_gain_5y, absolute_gain_total) as absolute_gain_5y,
       absolute_gain_total,
       relative_gain_1d,
       relative_gain_1w,
       relative_gain_1m,
       relative_gain_3m,
       relative_gain_1y,
       relative_gain_5y,
       relative_gain_total,
       now()                                           as updated_at
from gains
         left join relative_gains using (profile_id)
