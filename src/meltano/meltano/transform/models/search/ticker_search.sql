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
    select symbol, avg(volume) as daily_volume_avg
    from {{ ref('historical_prices') }}
    where "date"::date >= NOW() - interval '30 days'
    group by symbol
)
select t.symbol,
       t.name,
       ticker_search_alternative_names.name as alternative_name,
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
         left join {{ source('gainy', 'ticker_search_alternative_names') }}
                   on ticker_search_alternative_names.symbol = t.symbol
                       and (select max(_sdc_batched_at) from {{ source('gainy', 'ticker_search_alternative_names') }}) - ticker_search_alternative_names._sdc_batched_at < interval '1 minute'
where true
{% if not var('search_crypto_enabled') %}
  and t.type != 'crypto'
{% endif %}
{% if not var('search_index_enabled') %}
  and t.type != 'index'
{% endif %}
