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


with dphh_groupped as
         (
             select *,
                    case
                        when symbol like 'CUR:%'
                            then profile_id || '_cash_' || symbol
                        when collection_id is null
                            then 'ticker_' || profile_id || '_' || symbol
                        else 'ttf_' || profile_id || '_' || collection_id
                        end as holding_group_id
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
         ),
     last_selloff_date as materialized
         (
             select holding_group_id,
                    case
                        when bool_or(last_selloff_date is null)
                            then null
                        else min(last_selloff_date)
                        end as last_selloff_date
             from (
                      select holding_group_id, holding_id_v2, max(date) as last_selloff_date
                      from dphh_groupped
                      where value < 1e-3
                      group by holding_group_id, holding_id_v2
                  ) t
                      join {{ ref('drivewealth_holdings') }} using (holding_id_v2)
             group by holding_group_id
     ),
     data as
         (
             select dphh_groupped.profile_id,
                    holding_group_id,
                    date,
                    sum(value)               as value,
                    sum(prev_value)::numeric as prev_value,
                    sum(case when is_last_date.holding_id_v2 is null then 0 else value end -
                        cash_flow)::numeric  as cash_flow,
                    sum(cash_flow)::numeric  as cash_flow_sum
             from dphh_groupped
                      left join (
                                    select holding_id_v2, max(date) as date
                                    from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                    where not is_premarket
                                    group by holding_id_v2
                                ) is_last_date using (holding_id_v2, date)
                      left join last_selloff_date using (holding_group_id)
             where not is_premarket
               and (date > last_selloff_date or last_selloff_date is null)
             group by dphh_groupped.profile_id, holding_group_id, date
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
                       end as relative_gain_1d,
                   value - prev_value - cash_flow as absolute_gain_1d
             from data
             where date >= now()::date - interval '1 week'
             order by profile_id desc, holding_group_id desc, date desc
     ),
     data_1w as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(array_agg(cash_flow order by date), array_agg(date order by date))) ^
                    (count(date) / 365.0) - 1 as relative_gain_1w,
                    sum(cash_flow)            as absolute_gain_1w
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
                    (count(date) / 365.0) - 1 as relative_gain_1m,
                    sum(cash_flow)            as absolute_gain_1m
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
                    (count(date) / 365.0) - 1 as relative_gain_3m,
                    sum(cash_flow)            as absolute_gain_3m
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
                    (count(date) / 365.0) - 1 as relative_gain_1y,
                    sum(cash_flow)            as absolute_gain_1y
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
                    (count(date) / 365.0) - 1 as relative_gain_5y,
                    sum(cash_flow)            as absolute_gain_5y
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
                    (count(date) / 365.0) - 1 as relative_gain_total,
                    sum(cash_flow)            as absolute_gain_total
             from data
             group by profile_id, holding_group_id
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
