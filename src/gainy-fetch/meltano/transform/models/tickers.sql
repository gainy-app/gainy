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
	from {{ source('eod', 'eod_historical_prices') }}
	where "date"::date >= NOW() - interval '30 days'
	group by code
)
select t.*
from {{ ref('base_tickers') }} t
    join volumes v
        on t.symbol = v.symbol
where (v.avg_volume is not null and v.avg_volume >= {{ min_daily_volume }}) and (t.description is not null or length(t.description) < 5)

