{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = data_checks.period
        and data_checks.updated_at < dc_stats.max_updated_at',
    ]
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
     tickers_and_options as
         (
             select symbol, exchange_canonical, country_name
             from {{ ref('tickers') }}
             union all
             select contract_name, exchange_canonical, country_name
             from {{ ref('ticker_options_monitored') }}
                      join {{ ref('tickers') }} using (symbol)
         ),
     old_historical_prices as
         (
             select symbol
             from (
                      select tickers.symbol,
                             max(historical_prices.date) as date
                      from {{ ref('tickers') }}
                               left join {{ ref('historical_prices') }} on historical_prices.code = tickers.symbol
                      where volume > 0
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
         ),
     old_realtime_prices as
         (
             select symbol
             from (
                      select tickers.symbol,
                             max(eod_intraday_prices.time) as time
                      from {{ ref('tickers') }}
                               left join {{ source('eod', 'eod_intraday_prices') }} using (symbol)
                      group by tickers.symbol
                  ) t
                      left join old_historical_prices using (symbol)
                      join {{ ref('tickers') }} using (symbol)
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers.exchange_canonical
                               or (tickers.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers.country_name))
             where old_historical_prices.symbol is null
               and (t.time is null or least(now(), latest_trading_day.close_at) - t.time > interval '16 minutes')
-- Polygon delay is 15 minutes
         ),
     matchscore_distinct_tickers as
         (
             select symbol
             from {{ source('app', 'profile_ticker_match_score') }}
             group by symbol
         ),
     errors as
         (
{% if not var('realtime') %}
             select symbol,
                    'ttf_ticker_no_interest' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_interests') }} using (symbol)
                      left join {{ ref('interests') }} on interests.id = ticker_interests.interest_id
             where interests.id is null

             union all

             select symbol,
                   'ttf_ticker_no_industry' as code,
                   'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_industries') }} using (symbol)
                      left join {{ ref('gainy_industries') }} on gainy_industries.id = ticker_industries.industry_id
             where gainy_industries.id is null

             union all

             select collection_distinct_tickers.symbol,
                    'ttf_ticker_hidden' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('tickers') }} using (symbol)
             where tickers.symbol is null
           
             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_risk_score' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_risk_scores') }} using (symbol)
             where ticker_risk_scores.symbol is null
           
             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_category_continuous' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_categories_continuous') }} using (symbol)
             where ticker_categories_continuous.symbol is null

             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_matchscore' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join matchscore_distinct_tickers using (symbol)
                               left join {{ ref('tickers') }} using (symbol)
             where matchscore_distinct_tickers.symbol is null
             and tickers.type <> 'crypto'

             union all

             select symbol,
                    'old_historical_prices' as code,
                    'daily' as period
             from old_historical_prices

             union all
{% endif %}
             select tickers_and_options.symbol,
                    'old_realtime_metrics' as code,
                    'realtime' as period
             from tickers_and_options
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers_and_options.exchange_canonical
                               or (tickers_and_options.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers_and_options.country_name))
                      left join {{ ref('ticker_realtime_metrics') }} on ticker_realtime_metrics.symbol = tickers_and_options.symbol
             where ticker_realtime_metrics.symbol is null
                or least(now(), latest_trading_day.close_at) - ticker_realtime_metrics.time > interval '30 minutes'

             union all

             select symbol,
                    'old_realtime_chart' as code,
                    'realtime' as period
             from (
                      select tickers_and_options.symbol,
                             max(chart.datetime) as datetime
                      from tickers_and_options
                               left join {{ ref('chart') }}
                                         on chart.symbol = tickers_and_options.symbol
                                             and chart.period = '1d'
                      group by tickers_and_options.symbol
                  ) t
                      left join old_historical_prices using (symbol)
                      left join old_realtime_prices using (symbol)
                      join tickers_and_options using (symbol)
                      join latest_trading_day
                           on (latest_trading_day.exchange_name = tickers_and_options.exchange_canonical
                               or (tickers_and_options.exchange_canonical is null
                                   and latest_trading_day.country_name = tickers_and_options.country_name))
             where old_historical_prices.symbol is null
               and old_realtime_prices.symbol is null
               and (datetime is null or least(now(), latest_trading_day.close_at) - datetime > interval '30 minutes')

             union all

             select symbol,
                    'old_realtime_prices' as code,
                    'realtime' as period
             from old_realtime_prices
         )
select (code || '_' || symbol)::varchar as id,
       symbol,
       code::varchar,
       period::varchar,
       case
           when code = 'ttf_ticker_no_interest'
               then 'TTF tickers ' || symbol || ' is not linked to any interest.'
           when code = 'ttf_ticker_no_industry'
               then 'TTF tickers ' || symbol || ' is not linked to any industry.'
           when code = 'ttf_ticker_hidden'
               then 'TTF tickers ' || symbol || ' not present in the tickers table.'
           when code = 'ttf_ticker_no_risk_score'
               then 'TTF tickers ' || symbol || ' not present in the ticker_risk_scores table.'
           when code = 'ttf_ticker_no_category_continuous'
               then 'TTF tickers ' || symbol || ' not present in the ticker_categories_continuous table.'
           when code = 'ttf_ticker_no_matchscore'
               then 'TTF tickers ' || symbol || ' not present in the app.profile_ticker_match_score table.'
           when code = 'old_realtime_metrics'
               then 'Tickers ' || symbol || ' has old realtime metrics.'
           when code = 'old_realtime_chart'
               then 'Tickers ' || symbol || ' has old realtime chart.'
           when code = 'old_historical_prices'
               then 'Tickers ' || symbol || ' has old historical prices.'
           when code = 'old_realtime_prices'
               then 'Tickers ' || symbol || ' has old realtime prices.'
           end as message,
       now() as updated_at
from errors
