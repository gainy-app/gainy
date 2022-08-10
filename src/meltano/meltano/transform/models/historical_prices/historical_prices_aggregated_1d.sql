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


with uniq_tickers as
         (
             select symbol, min(date) as min_date
             from {{ ref('historical_prices') }}
             group by symbol
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
                 SELECT symbol, date_series::date as date
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
                 SELECT symbol, date_series::date as date
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
                 SELECT symbol, date_series::date as date
                 FROM generate_series(now() - interval '1 year' - interval '1 week', now(), '1 day') date_series
                          join filtered_base_tickers on true
                          join uniq_tickers using (symbol)
                 where date_series::date >= min_date
             )
         ),
     all_rows as
         (
             select tds.symbol || '_' || tds.date as id,
                    tds.symbol,
                    tds.date::timestamp         as datetime,
                    hp.open,
                    hp.high,
                    hp.low,
                    hp.close,
                    coalesce(hp.adjusted_close,
                             LAST_VALUE_IGNORENULLS(hp.adjusted_close) over (lookback)
                             )                           as adjusted_close,
                    coalesce(hp.updated_at,
                             LAST_VALUE_IGNORENULLS(hp.updated_at) over (lookback)
                             )                           as updated_at,
                    coalesce(volume, 0)                  as volume
             from tickers_dates_skeleton tds
                      left join {{ ref('historical_prices') }} hp using (symbol, "date")
                 window
                     lookback as (partition by tds.symbol order by tds."date" asc)
         )
select all_rows.*
from all_rows
{% if is_incremental() %}
         left join {{ this }} old_data using (symbol, datetime)
where old_data.symbol is null -- no old data
   or (all_rows.updated_at is not null and old_data.updated_at is null) -- old data is null and new is not
   or all_rows.updated_at > old_data.updated_at -- new data is newer than the old one
{% endif %}
-- Execution Time: 96290.198 ms
-- OK created incremental model historical_prices_aggregated_1d SELECT 4385623 in 152.88s
-- OK created incremental model historical_prices_aggregated_1d SELECT 4385623 in 143.39s
