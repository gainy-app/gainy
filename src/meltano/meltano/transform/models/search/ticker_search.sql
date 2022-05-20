{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with ticker_categories as (
    select symbol, array_agg(c.name) as ticker_categories
    from {{ ref('ticker_categories') }} tc
             join {{ ref('categories') }} c
                  on tc.category_id = c.id
    group by symbol
),
ticker_industries as (
    select symbol, array_agg(gainy_industries.name) as ticker_industries
    from {{ ref('ticker_industries') }} ti
        join {{ ref('gainy_industries') }}
            on ti.industry_id = gainy_industries.id
    group by symbol
),
ticker_volumes as (
    select code as symbol, avg(volume) as daily_volume_avg
    from {{ ref('historical_prices') }}
    where "date"::date >= NOW() - interval '30 days'
    group by code
)
select t.symbol,
       t.name,
       t.description,
       ti.ticker_industries as tag_1,
       tc.ticker_categories as tag_2,
       tv.daily_volume_avg::real
from {{ ref('tickers') }} t
         left join ticker_volumes tv
                   on t.symbol = tv.symbol
         left join ticker_industries ti
                   on t.symbol = ti.symbol
         left join ticker_categories tc
                   on t.symbol = tc.symbol
{% if not var('search_crypto_enabled') %}
where t.type != 'crypto'
{% endif %}
