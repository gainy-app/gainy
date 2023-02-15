{{
  config(
    materialized = "view",
  )
}}

with ticker_categories as
         (
             select symbol, array_agg(c.name) as ticker_categories
             from {{ ref('ticker_categories') }} tc
                      join {{ ref('categories') }} c
                           on tc.category_id = c.id
             group by symbol
         ),
    ticker_industries as
         (
             select symbol, array_agg(gainy_industries.name) as ticker_industries
             from {{ ref('ticker_industries') }} ti
                 join {{ ref('gainy_industries') }}
                     on ti.industry_id = gainy_industries.id
             group by symbol
         ),
    ticker_volumes as
         (
             select symbol, avg(volume) as daily_volume_avg
             from {{ ref('historical_prices') }}
             where "date"::date >= NOW() - interval '30 days'
             group by symbol
         ),
    tickers_with_realtime_prices as
         (
             select symbol
             from {{ ref('historical_intraday_prices') }}
                      join {{ ref('base_tickers') }} using (symbol)
             where type in ('crypto', 'index')
             group by symbol
         ),
     ticker_search_alternative_names as
        (
            select *
            from{{ source('gainy', 'ticker_search_alternative_names') }}
            where (select max(_sdc_batched_at) from {{ source('gainy', 'ticker_search_alternative_names') }}) - ticker_search_alternative_names._sdc_batched_at < interval '1 minute'
        )
select tickers.symbol,
       tickers.name,
       coalesce(
           ticker_search_alternative_names.name,
           regexp_replace(symbol, '\.(CC|INDX)$', '')
           )                               as alternative_name,
       tickers.description,
       ticker_industries.ticker_industries as tag_1,
       ticker_categories.ticker_categories as tag_2,
       ticker_volumes.daily_volume_avg::real
from {{ ref('tickers') }}
         left join tickers_with_realtime_prices using (symbol)
         left join ticker_volumes using (symbol)
         left join ticker_industries using (symbol)
         left join ticker_categories using (symbol)
         left join ticker_search_alternative_names using (symbol)
where (type not in ('crypto', 'index') or tickers_with_realtime_prices.symbol is not null)
{% if not var('crypto_enabled') %}
  and tickers.type != 'crypto'
{% endif %}
{% if not var('index_enabled') %}
  and tickers.type != 'index'
{% endif %}
