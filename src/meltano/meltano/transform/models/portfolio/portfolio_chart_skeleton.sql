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
{% if is_incremental() %}
with old_stats as materialized
     (
         select distinct on (
             profile_id,
             period
             ) profile_id,
               period,
               datetime as max_datetime,
               holding_count
         from {{ this }}
         order by profile_id desc, period desc, datetime desc
     )
{% endif %}

select distinct on (
    profile_id, period, datetime
    ) profile_id,
      period,
      datetime,
      t.holding_count,
      profile_id || '_' || period || '_' || datetime as id
from (
         select t.*,
                max(holding_count)
                over (partition by profile_id, period order by datetime rows between unbounded preceding and current row ) as cum_max_holding_count
         from (
                  select profile_id,
                         period,
                         datetime,
                         count(distinct holding_id_v2) as holding_count
                  from {{ ref('portfolio_holding_chart') }}

{% if is_incremental() %}
                           left join old_stats using (profile_id, period)
                  where old_stats.max_datetime is null or datetime > max_datetime
{% endif %}

                  group by profile_id, period, date, datetime

{% if is_incremental() %}
                  union all

                  select profile_id,
                         period,
                         max_datetime  as datetime,
                         holding_count
                  from old_stats
{% endif %}
              ) t
     ) t

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, period, datetime)
{% endif %}

where t.holding_count = cum_max_holding_count

{% if is_incremental() %}
  and old_data.profile_id is null
{% endif %}
