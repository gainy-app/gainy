{{
  config(
    materialized = "view",
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
                             sum(cash_flow)  as cash_flow,
                             max(updated_at) as updated_at
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
                                 cash_flow  as cash_flow_sum_1d,
                                 profile_stats.updated_at
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
                    cash_flow_sum_total,
                    updated_at
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
                    actual_value - prev_value_1d - cash_flow_sum_1d as absolute_gain_1d,
                    actual_value - prev_value_1w - cash_flow_sum_1w as absolute_gain_1w,
                    actual_value - prev_value_1m - cash_flow_sum_1m as absolute_gain_1m,
                    actual_value - prev_value_3m - cash_flow_sum_3m as absolute_gain_3m,
                    actual_value - prev_value_1y - cash_flow_sum_1y as absolute_gain_1y,
                    actual_value - prev_value_5y - cash_flow_sum_5y as absolute_gain_5y,
                    actual_value - cash_flow_sum_total              as absolute_gain_total,
                    updated_at
             from profile_values_marked
     )
select profile_id,
       coalesce(absolute_gain_1d, absolute_gain_total) as absolute_gain_1d,
       coalesce(absolute_gain_1w, absolute_gain_total) as absolute_gain_1w,
       coalesce(absolute_gain_1m, absolute_gain_total) as absolute_gain_1m,
       coalesce(absolute_gain_3m, absolute_gain_total) as absolute_gain_3m,
       coalesce(absolute_gain_1y, absolute_gain_total) as absolute_gain_1y,
       coalesce(absolute_gain_5y, absolute_gain_total) as absolute_gain_5y,
       absolute_gain_total,
       updated_at
from gains
