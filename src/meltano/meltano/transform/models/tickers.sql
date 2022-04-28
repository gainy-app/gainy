{% set min_daily_volume = 100000 %}

{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})::date',
    ]
  )
}}

with volumes as (
    select code as symbol, avg(volume) as avg_volume
    from {{ source('eod', 'eod_historical_prices') }}
    where "date"::date >= NOW() - interval '30 days'
    group by code
),
     latest_price as (
         select code as symbol, max(date) as date
         from {{ source('eod', 'eod_historical_prices') }}
         group by code
     )
select t.symbol,
       t.type,
       t.name,
       t.description,
       t.phone,
       t.logo_url,
       t.web_url,
       t.ipo_date,
       t.sector,
       t.industry,
       t.gic_sector,
       t.gic_group,
       t.gic_industry,
       t.gic_sub_industry,
       t.exchange,
       t.exchange_canonical,
       t.country_name,
       now()::timestamp as updated_at
from {{ ref('base_tickers') }} t
    join volumes v on t.symbol = v.symbol
    join latest_price on t.symbol = latest_price.symbol
where v.avg_volume is not null
  and v.avg_volume >= {{ min_daily_volume }}
  and t.description is not null
  and length (t.description) >= 5
  and latest_price.date is not null
  and latest_price.date::date >= now() - interval '7 days'
  and type in ('fund', 'etf', 'mutual fund', 'preferred stock', 'common stock', 'crypto')
