{{
  config(
    materialized = "incremental",
    tags = ["realtime"],
    post_hook=[
      index(['profile_id', 'holding_group_id', 'updated_at'], false),
      'delete from {{ this }}
        using (select profile_id, holding_group_id, max(updated_at) as updated_at from {{ this }} group by profile_id, holding_group_id) old_stats
        where {{ this }}.profile_id = old_stats.profile_id
          and {{ this }}.holding_group_id = old_stats.holding_group_id
          and {{ this }}.updated_at < old_stats.updated_at',
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
             from (select holding_group_id, holding_id_v2 from dphh_groupped group by holding_group_id, holding_id_v2) t1
                      left join (
                                    select holding_group_id, max(date) as last_selloff_date
                                    from (
                                             select holding_group_id, date
                                             from dphh_groupped
                                             group by holding_group_id, date
                                             having sum(value) < {{ var('price_precision') }}
                                         ) t
                                    group by holding_group_id
                                ) t2 using (holding_group_id)
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
                    max(updated_at)          as updated_at
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
{% if var('realtime') %}
     old_data_stats as
         (
             select profile_id, holding_group_id, max(updated_at) as updated_at
             from {{ this }}
             group by profile_id, holding_group_id
         ),
     data_stats as
         (
             select profile_id, holding_group_id, max(updated_at) as updated_at
             from data
             group by profile_id, holding_group_id
         ),
     filtered_holding_groups as
         (
             select profile_id, holding_group_id
             from data_stats
                      left join old_data_stats using (profile_id, holding_group_id)
             where data_stats.updated_at > old_data_stats.updated_at
                or old_data_stats.updated_at is null
         ),
{% endif %}

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
                   cash_flow - prev_value as absolute_gain_1d
             from data
{% if var('realtime') %}
                      join filtered_holding_groups using (profile_id, holding_group_id)
{% endif %}
             where date >= now()::date - interval '1 week'
             order by profile_id desc, holding_group_id desc, date desc
     ),
     data_1w as
         (
             select profile_id,
                    holding_group_id,
                    cnt                                   as count_1w,
                    (1 + xirr(cf, d)) ^ (cnt / 365.0) - 1 as relative_gain_1w,
                    absolute_gain_1w
             from (
                      select profile_id,
                             holding_group_id,
                             count(date)                        as count_1w,
                             array_agg(cash_flow order by date) as cf,
                             array_agg(date order by date)      as d,
                             count(date)                        as cnt,
                             sum(cash_flow)                     as absolute_gain_1w
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
{% if var('realtime') %}
                                        join filtered_holding_groups using (profile_id, holding_group_id)
{% endif %}
                               where date >= now()::date - interval '1 week'
                           ) t
                     group by profile_id, holding_group_id
                  ) t
     ),
     data_1m as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(cf, d)) ^ (cnt / 365.0) - 1 as relative_gain_1m,
                    absolute_gain_1m
             from (
                      select profile_id,
                             holding_group_id,
                             array_agg(cash_flow order by date) as cf,
                             array_agg(date order by date)      as d,
                             count(date)                        as cnt,
                             sum(cash_flow)                     as absolute_gain_1m
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
{% if var('realtime') %}
                                        join filtered_holding_groups using (profile_id, holding_group_id)
{% endif %}
                               where date >= now()::date - interval '1 month'
                           ) t
                      group by profile_id, holding_group_id
                  ) t
     ),
     data_3m as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(cf, d)) ^ (cnt / 365.0) - 1 as relative_gain_3m,
                    absolute_gain_3m
             from (
                      select profile_id,
                             holding_group_id,
                             array_agg(cash_flow order by date) as cf,
                             array_agg(date order by date)      as d,
                             count(date)                        as cnt,
                             sum(cash_flow)                     as absolute_gain_3m
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
{% if var('realtime') %}
                                        join filtered_holding_groups using (profile_id, holding_group_id)
{% endif %}
                               where date >= now()::date - interval '3 month'
                           ) t
                      group by profile_id, holding_group_id
                  ) t
     ),
     data_1y as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(cf, d)) ^ (cnt / 365.0) - 1 as relative_gain_1y,
                    absolute_gain_1y
             from (
                      select profile_id,
                             holding_group_id,
                             array_agg(cash_flow order by date) as cf,
                             array_agg(date order by date)      as d,
                             count(date)                        as cnt,
                             sum(cash_flow)                     as absolute_gain_1y
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
{% if var('realtime') %}
                                        join filtered_holding_groups using (profile_id, holding_group_id)
{% endif %}
                               where date >= now()::date - interval '1 year'
                           ) t
                      group by profile_id, holding_group_id
                  ) t
     ),
     data_5y as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(cf, d)) ^ (cnt / 365.0) - 1 as relative_gain_5y,
                    absolute_gain_5y
             from (
                      select profile_id,
                             holding_group_id,
                             array_agg(cash_flow order by date) as cf,
                             array_agg(date order by date)      as d,
                             count(date)                        as cnt,
                             sum(cash_flow)                     as absolute_gain_5y
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
{% if var('realtime') %}
                                        join filtered_holding_groups using (profile_id, holding_group_id)
{% endif %}
                               where date >= now()::date - interval '5 year'
                           ) t
                      group by profile_id, holding_group_id
                  ) t
     ),
     data_total as
         (
             select profile_id,
                    holding_group_id,
                    (1 + xirr(cf, d)) ^ (cnt / 365.0) - 1 as relative_gain_total,
                    absolute_gain_total,
                    updated_at
             from (
                      select profile_id,
                             holding_group_id,
                             array_agg(cash_flow order by date) as cf,
                             array_agg(date order by date)      as d,
                             count(date)                        as cnt,
                             sum(cash_flow)                     as absolute_gain_total,
                             max(updated_at)                    as updated_at
                      from data
{% if var('realtime') %}
                               join filtered_holding_groups using (profile_id, holding_group_id)
{% endif %}
                      group by profile_id, holding_group_id
                  ) t
     )

select profile_id,
       holding_group_id,
       actual_value,
       relative_gain_1d,
       case
           when relative_gain_1w is null or count_1w = 1
               then relative_gain_1d
           when relative_gain_1w is not null
               then relative_gain_1w
           end             as relative_gain_1w,
       case
           when relative_gain_1m is null or count_1w = 1
               then relative_gain_1d
           when relative_gain_1m is not null
               then relative_gain_1m
           end             as relative_gain_1m,
       case
           when relative_gain_3m is null or count_1w = 1
               then relative_gain_1d
           when relative_gain_3m is not null
               then relative_gain_3m
           end             as relative_gain_3m,
       case
           when relative_gain_1y is null or count_1w = 1
               then relative_gain_1d
           when relative_gain_1y is not null
               then relative_gain_1y
           end             as relative_gain_1y,
       case
           when relative_gain_5y is null or count_1w = 1
               then relative_gain_1d
           when relative_gain_5y is not null
               then relative_gain_5y
           end             as relative_gain_5y,
       case
           when relative_gain_total is null or count_1w = 1
               then relative_gain_1d
           when relative_gain_total is not null
               then relative_gain_total
           end             as relative_gain_total,
       absolute_gain_1d,
       absolute_gain_1w,
       absolute_gain_1m,
       absolute_gain_3m,
       absolute_gain_1y,
       absolute_gain_5y,
       absolute_gain_total,
       0::double precision as ltt_quantity_total, -- TODO calculate
       updated_at
from data_total
         left join data_1d using (profile_id, holding_group_id)
         left join data_1w using (profile_id, holding_group_id)
         left join data_1m using (profile_id, holding_group_id)
         left join data_3m using (profile_id, holding_group_id)
         left join data_1y using (profile_id, holding_group_id)
         left join data_5y using (profile_id, holding_group_id)
