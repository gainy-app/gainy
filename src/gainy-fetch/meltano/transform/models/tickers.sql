{% set min_daily_volume = 100000 %}

{{
  config(
    materialized = "table",
    unique_key = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with volumes as (
	select code as symbol, avg(volume) as avg_volume
	from {{ source('eod','raw_historical_prices') }}
	where "date"::date >= NOW() - interval '30 days'
	group by code
)
select t.symbol, t."type", t."name", t.description, t.phone, t.logo_url, t.web_url, t.ipo_date, t.sector, t.industry,
       gic_sector, gic_group, gic_industry, gic_sub_industry, country_name, updated_at
from {{ ref('base_tickers') }} t
    left join volumes v
        on t.symbol = v.symbol
where v.avg_volume >= {{ min_daily_volume }} and t.description is not null

