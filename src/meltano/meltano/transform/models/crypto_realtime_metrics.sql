{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    tags = ["realtime"],
    post_hook=[
      pk('symbol'),
    ]
  )
}}

with today_stats as
         (
             select symbol,
                    max(high)   as high,
                    min(low)    as low,
                    sum(volume) as volume
             from {{ ref('chart') }}
                      join {{ ref('base_tickers') }} using (symbol)
             where period = '1d'
               and type = 'crypto'
             group by symbol
         )
select distinct on (
    base_tickers.symbol
    ) base_tickers.symbol,
      case
          when coingecko_market_realtime.last_updated::timestamp > coingecko_coin.last_updated::timestamp
              then coingecko_market_realtime.ath
          else (coingecko_coin.market_data -> 'ath' ->> 'usd')::double precision
          end            as ath,
      case
          when coingecko_market_realtime.last_updated::timestamp > coingecko_coin.last_updated::timestamp
              then coingecko_market_realtime.atl
          else (coingecko_coin.market_data -> 'atl' ->> 'usd')::double precision
          end            as atl,
      case
          when coingecko_market_realtime.last_updated::timestamp > coingecko_coin.last_updated::timestamp
              then coingecko_market_realtime.market_cap
          else ticker_realtime_metrics.actual_price *
               (coingecko_coin.market_data ->> 'circulating_supply')::double precision
          end            as market_capiptalization,

      case
          when coingecko_market_realtime.last_updated::timestamp > coingecko_coin.last_updated::timestamp
              then coingecko_market_realtime.circulating_supply
          else (coingecko_coin.market_data ->> 'circulating_supply')::double precision
          end            as circulating_supply,
      case
          when coingecko_market_realtime.last_updated::timestamp > coingecko_coin.last_updated::timestamp
              then coingecko_market_realtime.max_supply
          else (coingecko_coin.market_data ->> 'max_supply')::double precision
          end            as max_supply,
      case
          when coingecko_market_realtime.last_updated::timestamp > coingecko_coin.last_updated::timestamp
              then coingecko_market_realtime.total_supply
          else (coingecko_coin.market_data ->> 'total_supply')::double precision
          end            as total_supply,

      today_stats.volume as volume_24h,
      today_stats.high   as high_24h,
      today_stats.low    as low_24h
from {{ source('coingecko', 'coingecko_market_realtime') }}
         join {{ source('coingecko', 'coingecko_coin') }} on coingecko_coin.id = coingecko_market_realtime.id
         join {{ ref('base_tickers') }} on base_tickers.symbol = upper(coingecko_coin.symbol) || '.CC'
         left join {{ ref('ticker_realtime_metrics') }} on ticker_realtime_metrics.symbol = base_tickers.symbol
         left join today_stats on today_stats.symbol = base_tickers.symbol
where base_tickers.type = 'crypto'
order by base_tickers.symbol, coingecko_rank
