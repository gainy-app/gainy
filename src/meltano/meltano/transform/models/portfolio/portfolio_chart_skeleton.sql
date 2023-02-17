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


-- Execution Time: 44214.629 ms
select t.*,
       profile_id || '_' || period || '_' || datetime as id
from (
         select distinct on (
             profile_id, period, datetime
             ) profile_id,
               period,
               datetime,
               t.holding_count
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
                           where period in ('1m', '3m', '1y', '5y', 'all')
                           group by profile_id, period, datetime
                       ) t
              ) t
         where t.holding_count = cum_max_holding_count

         union all

         select distinct on (
             profile_id, period, datetime
             ) profile_id,
               period,
               datetime,
               null::int as holding_count
         from {{ ref('portfolio_holding_chart') }}
                  join {{ ref('profile_holdings_normalized_all') }} using (profile_id, holding_id_v2)
                  join {{ ref('week_trading_sessions_static') }} using (symbol, date)
         where period in ('1d', '1w')
           and datetime between open_at and close_at - interval '1 second'
     ) t

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, period, datetime)
where old_data.profile_id is null
{% endif %}
