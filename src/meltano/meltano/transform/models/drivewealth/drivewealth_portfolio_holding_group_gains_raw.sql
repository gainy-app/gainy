{{
  config(
    materialized = "incremental",
    tags = ["realtime"],
    post_hook=[
      index(['profile_id', 'holding_group_id', 'updated_at'], false),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


with data as
         (
             select profile_id,
                    holding_group_id,
                    date,
                    sum(value)               as value,
                    sum(prev_value)::numeric as prev_value,
                    sum(case when is_last_date.profile_id is null then 0 else value end -
                        cash_flow)::numeric  as cash_flow
             from {{ ref('profile_holdings_normalized') }}
                      join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2)
                      left join (
                                    select profile_id, holding_id_v2, max(date) as date
                                    from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                    group by profile_id, holding_id_v2
                                ) is_last_date using (holding_id_v2, profile_id, date)
                      left join (
                                    select profile_id, holding_id_v2, max(date) as last_selloff_date
                                    from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                    where value < 1e-3
                                    group by profile_id, holding_id_v2
                                ) last_selloff_date using (profile_id, holding_id_v2)
             where date > last_selloff_date
                or last_selloff_date is null
             group by profile_id, holding_group_id, date
         ),
     data_1d as
         (
             select distinct on (
                 profile_id, holding_group_id
                 ) profile_id,
                   holding_group_id,
                   value   as actual_value,
                   case
                       when prev_value + (value - cash_flow) > 0
                           then value / (prev_value + (value - cash_flow)) - 1
                       when prev_value > 0
                           then (value - (value - cash_flow)) / prev_value - 1
                       end as relative_gain_1d
             from data
             where date >= now()::date - interval '1 week'
             order by profile_id desc, holding_group_id desc, date desc
     ),
     data_1w as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(array_agg(cash_flow order by date), array_agg(date order by date))) ^
                    (count(date) / 365.0) - 1 as relative_gain_1w
             from (
                      select profile_id,
                             holding_group_id,
                             date,
                             cash_flow - case
                                             when row_number()
                                                  over (partition by profile_id, holding_group_id order by date) = 1
                                                 then prev_value
                                             else 0 end as cash_flow
                      from data
                      where date >= now()::date - interval '1 week'
                  ) t
             group by profile_id, holding_group_id
     ),
     data_1m as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(array_agg(cash_flow order by date), array_agg(date order by date))) ^
                    (count(date) / 365.0) - 1 as relative_gain_1m
             from (
                      select profile_id,
                             holding_group_id,
                             date,
                             cash_flow - case
                                             when row_number()
                                                  over (partition by profile_id, holding_group_id order by date) = 1
                                                 then prev_value
                                             else 0 end as cash_flow
                      from data
                      where date >= now()::date - interval '1 month'
                  ) t
             group by profile_id, holding_group_id
     ),
     data_3m as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(array_agg(cash_flow order by date), array_agg(date order by date))) ^
                    (count(date) / 365.0) - 1 as relative_gain_3m
             from (
                      select profile_id,
                             holding_group_id,
                             date,
                             cash_flow - case
                                             when row_number()
                                                  over (partition by profile_id, holding_group_id order by date) = 1
                                                 then prev_value
                                             else 0 end as cash_flow
                      from data
                      where date >= now()::date - interval '3 month'
                  ) t
             group by profile_id, holding_group_id
     ),
     data_1y as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(array_agg(cash_flow order by date), array_agg(date order by date))) ^
                    (count(date) / 365.0) - 1 as relative_gain_1y
             from (
                      select profile_id,
                             holding_group_id,
                             date,
                             cash_flow - case
                                             when row_number()
                                                  over (partition by profile_id, holding_group_id order by date) = 1
                                                 then prev_value
                                             else 0 end as cash_flow
                      from data
                      where date >= now()::date - interval '1 year'
                  ) t
             group by profile_id, holding_group_id
     ),
     data_5y as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(array_agg(cash_flow order by date), array_agg(date order by date))) ^
                    (count(date) / 365.0) - 1 as relative_gain_5y
             from (
                      select profile_id,
                             holding_group_id,
                             date,
                             cash_flow - case
                                             when row_number()
                                                  over (partition by profile_id, holding_group_id order by date) = 1
                                                 then prev_value
                                             else 0 end as cash_flow
                      from data
                      where date >= now()::date - interval '5 year'
                  ) t
             group by profile_id, holding_group_id
     ),
     data_total as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(array_agg(cash_flow order by date), array_agg(date order by date))) ^
                    (count(date) / 365.0) - 1 as relative_gain_total
             from data
             group by profile_id, holding_group_id
     ),
     absolute_gains as
         (
             select profile_holdings_normalized.profile_id,
                    holding_group_id,
                    -- it's important to use actual_value from drivewealth_portfolio_historical_holdings_marked, otherwise gains may be inconsistent
                    sum(drivewealth_portfolio_historical_holdings_marked.actual_value - prev_value_1d - cash_flow_sum_1d) as absolute_gain_1d,
                    sum(drivewealth_portfolio_historical_holdings_marked.actual_value - prev_value_1w - cash_flow_sum_1w) as absolute_gain_1w,
                    sum(drivewealth_portfolio_historical_holdings_marked.actual_value - prev_value_1m - cash_flow_sum_1m) as absolute_gain_1m,
                    sum(drivewealth_portfolio_historical_holdings_marked.actual_value - prev_value_3m - cash_flow_sum_3m) as absolute_gain_3m,
                    sum(drivewealth_portfolio_historical_holdings_marked.actual_value - prev_value_1y - cash_flow_sum_1y) as absolute_gain_1y,
                    sum(drivewealth_portfolio_historical_holdings_marked.actual_value - prev_value_5y - cash_flow_sum_5y) as absolute_gain_5y,
                    sum(drivewealth_portfolio_historical_holdings_marked.actual_value - cash_flow_sum_total)              as absolute_gain_total
             from {{ ref('profile_holdings_normalized') }}
                      left join {{ ref('drivewealth_portfolio_historical_holdings_marked') }} using (holding_id_v2)
             group by profile_holdings_normalized.profile_id, holding_group_id
     )

select profile_id,
       holding_group_id,
       actual_value,
       relative_gain_1d,
       relative_gain_1w,
       relative_gain_1m,
       relative_gain_3m,
       relative_gain_1y,
       relative_gain_5y,
       relative_gain_total,
       absolute_gain_1d,
       absolute_gain_1w,
       absolute_gain_1m,
       absolute_gain_3m,
       absolute_gain_1y,
       absolute_gain_5y,
       absolute_gain_total,
       0::double precision as ltt_quantity_total, -- TODO calculate
       now()               as updated_at
from data_total
         left join data_1d using (profile_id, holding_group_id)
         left join data_1w using (profile_id, holding_group_id)
         left join data_1m using (profile_id, holding_group_id)
         left join data_3m using (profile_id, holding_group_id)
         left join data_1y using (profile_id, holding_group_id)
         left join data_5y using (profile_id, holding_group_id)
         left join absolute_gains using (profile_id, holding_group_id)
