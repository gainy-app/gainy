{% set min_daily_volume = 500000 %}

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

with exchange_month_trading_sessions_count as
         (
             select exchange_name, count(date) as count
             from {{ ref('exchange_schedule') }}
             where open_at between now() - interval '1 month' and now()
             group by exchange_name
         ),
     country_month_trading_sessions_count as
         (
             select country_name, count(date) as count
             from {{ ref('exchange_schedule') }}
             where open_at between now() - interval '1 month' and now()
             group by country_name
         ),
     volumes as
         (
             select symbol,
                    month_volume_sum / coalesce(exchange_month_trading_sessions_count.count,
                                                country_month_trading_sessions_count.count, 
                                                30) as avg_volume -- crypto dont has any country nor exchange, so let it be interval '30 days' as in month_volume_sum
             from (
                      select code                         as symbol,
                             sum(volume * adjusted_close) as month_volume_sum
                      from {{ source('eod', 'eod_historical_prices') }}
                      where "date"::date >= NOW() - interval '30 days'
                      group by code
                  ) t
                      join {{ ref('base_tickers') }} using (symbol)
                      left join exchange_month_trading_sessions_count
                                on exchange_month_trading_sessions_count.exchange_name = base_tickers.exchange_canonical
                      left join country_month_trading_sessions_count
                                on country_month_trading_sessions_count.country_name = base_tickers.country_name
         ),
     latest_price as
         (
             select code as symbol, max(date) as date
             from {{ source('eod', 'eod_historical_prices') }}
             group by code
         )
select base_tickers.symbol,
       base_tickers.type,
       base_tickers.name,
       base_tickers.description,
       base_tickers.phone,
       base_tickers.logo_url,
       base_tickers.web_url,
       base_tickers.ipo_date,
       base_tickers.sector,
       base_tickers.industry,
       base_tickers.gic_sector,
       base_tickers.gic_group,
       base_tickers.gic_industry,
       base_tickers.gic_sub_industry,
       base_tickers.exchange,
       base_tickers.exchange_canonical,
       base_tickers.country_name,
       now()::timestamp as updated_at
from {{ ref('base_tickers') }}
         join volumes using (symbol)
         join latest_price using (symbol)
where volumes.avg_volume is not null
  and volumes.avg_volume >= {{ min_daily_volume }}
  and base_tickers.description is not null
  and length(base_tickers.description) >= 5
  and latest_price.date is not null
  and latest_price.date::date >= now() - interval '7 days'
  and type in ('fund', 'etf', 'mutual fund', 'preferred stock', 'common stock', 'crypto')
