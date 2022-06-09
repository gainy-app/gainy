{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('industry_id, period, datetime'),
      index(this, 'id', true),
      'with min_date as (select period, min(datetime) as datetime from {{ ref("chart") }} group by period)
       delete from {{ this }}
       using min_date
       where {{ this }}.period = min_date.period and {{ this }}.datetime < min_date.datetime',
    ]
  )
}}


with
{% if is_incremental() %}
     max_date as
         (
             select industry_id,
                    period,
                    max(datetime) as datetime
             from {{ this }}
             group by industry_id, period
         ),
{% endif %}
     chart_raw0 as
         (
             select industry_id,
                    period,
                    datetime,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY adjusted_close) as median_price,
                    count(*)                                                    as rows_cnt
             from {{ ref('chart') }}
                      join {{ ref('ticker_industries') }} using (symbol)
             group by industry_id, period, datetime
         ),
     chart_raw1 as
         (
             select industry_id,
                    period,
                    datetime,
                    median_price,
                    rows_cnt,
                    max(rows_cnt) over (partition by industry_id, period order by datetime)      as rolling_rows_cnt
             from chart_raw0
         ),
     chart_raw2 as
         (
             select industry_id,
                    period,
                    datetime,
                    median_price,
                    rows_cnt,
                    rolling_rows_cnt,
                    sum(case when rows_cnt = rolling_rows_cnt then 1 end)
                    over (partition by industry_id, period order by datetime) as grp
             from chart_raw1
         )
select (industry_id || '_' || period || '_' || chart_raw2.datetime)::varchar as id,
       industry_id,
       period,
       chart_raw2.datetime,
       case
           when rows_cnt != rolling_rows_cnt
               then first_value(median_price)
                    OVER (partition by industry_id, period, grp order by chart_raw2.datetime)
           else median_price
           end                                                    as median_price
from chart_raw2
{% if is_incremental() %}
         left join max_date using (industry_id, period)
where max_date.datetime is null or chart_raw2.datetime >= max_date.datetime - interval '20 minutes'
  {% if var('realtime') %}
  and period in ('1d', '1w')
  {% endif %}
{% endif %}
