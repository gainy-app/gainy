{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, datetime'),
      index(this, 'id', true),
      'create index if not exists "datetime__symbol" ON {{ this }} (datetime, symbol)',
    ]
  )
}}


-- 1d
-- Execution Time: 96290.198 ms
with uniq_tickers as
         (
             select code as symbol, min(date) as min_date
             from {{ ref('historical_prices') }}
             group by code
         ),
     tickers_dates_skeleton as
         (
             (
                 -- exchange tickers
                 with filtered_base_tickers as
                          (
                              select symbol, exchange_canonical
                              from {{ ref('base_tickers') }}
                              where type != 'crypto'
                                and exchange_canonical is not null
                              union all
                              select contract_name as symbol, exchange_canonical
                              from {{ ref('ticker_options_monitored') }}
                                       join {{ ref('base_tickers') }} using (symbol)
                              where exchange_canonical is not null
                          )
                 SELECT symbol as code, date_series::date as date
                 FROM generate_series(now() - interval '1 year' - interval '1 week', now(), '1 day') date_series
                          join filtered_base_tickers on true
                          join uniq_tickers using (symbol)
                          left join {{ ref('exchange_holidays') }}
                                    on exchange_holidays.exchange_name = filtered_base_tickers.exchange_canonical
                                        and exchange_holidays.date = date_series::date
                 where exchange_holidays.date is null
                   and extract(isodow from date_series) < 6
                   and date_series::date >= min_date
             )
             union all
             (
                 -- USA tickers without exchange
                 with filtered_base_tickers as
                          (
                              select symbol, country_name
                              from {{ ref('base_tickers') }}
                              where type != 'crypto'
                                and exchange_canonical is null
                                and (country_name in ('USA') or country_name is null)
                              union all
                              select contract_name as symbol, country_name
                              from {{ ref('ticker_options_monitored') }}
                                       join {{ ref('base_tickers') }} using (symbol)
                              where exchange_canonical is null
                                and (country_name in ('USA') or country_name is null)
                          )
                 SELECT symbol as code, date_series::date as date
                 FROM generate_series(now() - interval '1 year' - interval '1 week', now(), '1 day') date_series
                          join filtered_base_tickers on true
                          join uniq_tickers using (symbol)
                          left join {{ ref('exchange_holidays') }}
                                    on exchange_holidays.country_name = filtered_base_tickers.country_name
                                        and exchange_holidays.date = date_series::date
                 where exchange_holidays.date is null
                   and extract(isodow from date_series) < 6
                   and date_series::date >= min_date
             )
             union all
             (
                 -- crypto
                 with filtered_base_tickers as
                          (
                              select symbol, country_name
                              from {{ ref('base_tickers') }}
                              where type = 'crypto'
                          )
                 SELECT symbol as code, date_series::date as date
                 FROM generate_series(now() - interval '1 year' - interval '1 week', now(), '1 day') date_series
                          join filtered_base_tickers on true
                          join uniq_tickers using (symbol)
                 where date_series::date >= min_date
             )
         )
select tds.code || '_' || tds.date as id,
       tds.code                    as symbol,
       tds.date::timestamp         as datetime,
       hp.open,
       hp.high,
       hp.low,
       hp.close,
       coalesce(hp.adjusted_close,
                LAST_VALUE_IGNORENULLS(hp.adjusted_close) over (lookback)
                )                           as adjusted_close,
       coalesce(volume, 0)                  as volume
from tickers_dates_skeleton tds
         left join {{ ref('historical_prices') }} hp using (code, "date")
    window
        lookback as (partition by tds.code order by tds."date" asc)
