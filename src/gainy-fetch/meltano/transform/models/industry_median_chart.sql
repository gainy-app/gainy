{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "industry_id__period__datetime") }} (industry_id, period, datetime)',
      'with min_date as (select period, min(datetime) as datetime from {{ ref("chart") }} group by period)
       delete from {{ this }}
       using min_date
       where {{ this }}.period = min_date.period and {{ this }}.datetime < min_date.datetime',
    ]
  )
}}


{% if is_incremental() %}
with max_date as
         (
             select industry_id,
                    period,
                    max(datetime) as datetime
             from industry_median_chart
             group by industry_id, period
         )
{% endif %}
select (ti.industry_id || '_' || chart.period || '_' || chart.datetime)::varchar as id,
       ti.industry_id,
       chart.period,
       chart.datetime,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY adjusted_close)               as median_price
from {{ ref('chart') }}
         join {{ ref('ticker_industries') }} ti on chart.symbol = ti.symbol
{% if is_incremental() %}
         left join max_date
                   on max_date.industry_id = ti.industry_id
                       and max_date.period = chart.period
where max_date.datetime is null or chart.datetime >= max_date.datetime - interval '20 minutes'
  {% if var('realtime') %}
  and chart.period in ('1d', '1w')
  {% endif %}
{% endif %}
group by ti.industry_id, chart.period, chart.datetime