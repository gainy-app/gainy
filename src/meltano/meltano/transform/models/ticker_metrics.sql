{{
  config(
    materialized = "view",
  )
}}


select distinct on (
    symbol
    ) ticker_metrics_raw.*,
      ticker_realtime_metrics.price_change_1w,
      ticker_realtime_metrics.price_change_1m,
      ticker_realtime_metrics.price_change_3m,
      ticker_realtime_metrics.price_change_1y,
      ticker_realtime_metrics.price_change_5y,
      ticker_realtime_metrics.price_change_all,
      historical_prices_marked.price_1d  as prev_price_1d,
      historical_prices_marked.price_1w  as prev_price_1w,
      historical_prices_marked.price_1m  as prev_price_1m,
      historical_prices_marked.price_3m  as prev_price_3m,
      historical_prices_marked.price_1y  as prev_price_1y,
      historical_prices_marked.price_5y  as prev_price_5y,
      historical_prices_marked.price_all as prev_price_all
from {{ ref('ticker_metrics_raw') }}
         left join {{ ref('ticker_realtime_metrics') }} using (symbol)
         left join {{ ref('historical_prices_marked') }} using (symbol)
order by symbol desc, ticker_metrics_raw.updated_at desc