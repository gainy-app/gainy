{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('profile_id, period, datetime'),
      index('id', true),
    ],
  )
}}


-- Execution Time: 143330.918 ms
select distinct on (
    profile_id, period, datetime
    ) profile_id,
      period,
      datetime,
      profile_id || '_' || period || '_' || datetime as id
from (
         select t.*,
                max(cnt)
                over (partition by profile_id, period order by datetime rows between unbounded preceding and current row ) as cum_max_cnt
         from (
                  select profile_id,
                         period,
                         date,
                         datetime,
                         count(distinct holding_id_v2) as cnt
                  from {{ ref('portfolio_holding_chart') }}

{% if is_incremental() and var('realtime') %}
                           left join (
                                        select profile_id, period, max(datetime) as max_datetime
                                        from {{ this }}
                                        group by profile_id, period
                                     ) old_stats using (profile_id, period)
                  where period in ('1d', '1w')
                    and (old_stats.max_datetime is null or datetime > max_datetime)
{% endif %}

                  group by profile_id, period, date, datetime
              ) t
     ) t

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, period, datetime)
{% endif %}

where cnt = cum_max_cnt

{% if is_incremental() %}
  and old_data.profile_id is null
{% endif %}
