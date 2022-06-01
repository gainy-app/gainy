{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with collection_distinct_tickers as
         (
             select distinct symbol
             from {{ ref('ticker_collections') }}
                      join {{ ref('collections') }} on ticker_collections.collection_id = collections.id
             where collections.enabled = '1'
               and collections.personalized = '0'
         ),
     latest_trading_day as
         (
             select distinct on (exchange_name, country_name) *
             from {{ ref('exchange_schedule') }}
             where open_at < now()
             order by exchange_name, country_name, date desc
         ),
     errors as
         (
             select symbol,
                    'ttf_ticker_no_interest' as code
             from collection_distinct_tickers
                      left join {{ ref('ticker_interests') }} using (symbol)
                      left join {{ ref('interests') }} on interests.id = ticker_interests.interest_id
             where interests.id is null

             union all

             select symbol,
                    'ttf_ticker_no_industry' as code
             from collection_distinct_tickers
                      left join {{ ref('ticker_industries') }} using (symbol)
                      left join {{ ref('gainy_industries') }} on gainy_industries.id = ticker_industries.industry_id
             where gainy_industries.id is null

             union all

             select collection_distinct_tickers.symbol,
                    'ttf_ticker_hidden' as code
             from collection_distinct_tickers
                      left join {{ ref('tickers') }} on tickers.symbol = collection_distinct_tickers.symbol
             where tickers.symbol is null

             union all

             select tickers.symbol,
                    'old_realtime_metrics' as code
             from {{ ref('tickers') }}
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers.exchange_canonical
                               or (tickers.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers.country_name))
                      left join {{ ref('ticker_realtime_metrics') }} on ticker_realtime_metrics.symbol = tickers.symbol
             where ticker_realtime_metrics.symbol is null
                or least(now(), latest_trading_day.close_at) - ticker_realtime_metrics.time > interval '30 minutes'

             union all

             select symbol,
                    'old_realtime_chart' as code
             from (
                      select tickers.symbol,
                             max(chart.datetime) as datetime
                      from {{ ref('tickers') }}
                               left join {{ ref('chart') }}
                                         on chart.symbol = tickers.symbol
                                             and chart.period = '1d'
                      group by tickers.symbol
                  ) t
                      join {{ ref('tickers') }} using (symbol)
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers.exchange_canonical
                               or (tickers.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers.country_name))
             where datetime is null
                or least(now(), latest_trading_day.close_at) - datetime > interval '30 minutes'

             union all

             select symbol,
                    'old_historical_prices' as code
             from (
                      select tickers.symbol,
                             max(historical_prices.date) as date
                      from {{ ref('tickers') }}
                               left join {{ ref('historical_prices') }} on historical_prices.code = tickers.symbol
                      group by tickers.symbol
                  ) t
                      join {{ ref('tickers') }} using (symbol)
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers.exchange_canonical
                               or (tickers.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers.country_name))
             where t.date is null
                or latest_trading_day.date - t.date > (now() < latest_trading_day.date::timestamp + interval '28 hours')::int
-- US markets typically close at 20:00. Fetching starts at 2:00. We expect prices to be present in 2 hours after start.
             union all

             select symbol,
                    'old_realtime_prices' as code
             from (
                      select tickers.symbol,
                             max(eod_intraday_prices.time) as time
                      from {{ ref('tickers') }}
                               left join {{ source('eod', 'eod_intraday_prices') }} on eod_intraday_prices.symbol = tickers.symbol
                      group by tickers.symbol
                  ) t
                      join {{ ref('tickers') }} using (symbol)
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers.exchange_canonical
                               or (tickers.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers.country_name))
             where t.time is null
                or least(now(), latest_trading_day.close_at) - t.time > interval '16 minutes'
-- Polygon delay is 15 minutes
         )
select symbols,
       code::varchar,
       case
           when code = 'ttf_ticker_no_interest'
               then 'TTF tickers ' || symbols || ' is not linked to any interest.'
           when code = 'ttf_ticker_no_industry'
               then 'TTF tickers ' || symbols || ' is not linked to any industry.'
           when code = 'ttf_ticker_hidden'
               then 'TTF tickers ' || symbols || ' not present in the tickers table.'
           when code = 'old_realtime_metrics'
               then 'Tickers ' || symbols || ' has old realtime metrics.'
           when code = 'old_realtime_chart'
               then 'Tickers ' || symbols || ' has old realtime chart.'
           when code = 'old_historical_prices'
               then 'Tickers ' || symbols || ' has old realtime prices.'
           when code = 'old_realtime_prices'
               then 'Tickers ' || symbols || ' has old historical prices.'
           end as message
from (
         select json_agg(symbol) as symbols,
                code
         from errors
         group by code
     ) t
