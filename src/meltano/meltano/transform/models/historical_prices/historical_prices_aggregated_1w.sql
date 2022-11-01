{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, datetime'),
      index('id', true),
      'create index if not exists "hpa_1w_datetime__symbol" ON {{ this }} (datetime, symbol)',
    ]
  )
}}


-- Execution Time: 91466.942 ms
with combined_daily_prices as materialized
         (
             select DISTINCT ON (
                 t.symbol,
                 t.date
                 ) t.*
             from (
                      (
                          select DISTINCT ON (
                              date_week,
                              symbol
                              ) symbol as symbol,
                                date_week                                                                                          as date,
                                first_value(open)
                                OVER (partition by date_week, symbol order by date rows between current row and unbounded following) as open,
                                max(high)
                                OVER (partition by date_week, symbol order by date rows between current row and unbounded following) as high,
                                min(low)
                                OVER (partition by date_week, symbol order by date rows between current row and unbounded following) as low,
                                last_value(close)
                                OVER (partition by date_week, symbol order by date rows between current row and unbounded following) as close,
                                last_value(adjusted_close)
                                OVER (partition by date_week, symbol order by date rows between current row and unbounded following) as adjusted_close,
                                sum(volume)
                                OVER (partition by date_week, symbol order by date rows between current row and unbounded following) as volume,
                                0                                                                                                  as priority,
                                updated_at
                          from {{ ref('historical_prices') }}
                          where date_week >= now() - interval '5 year' - interval '1 week'
                          order by date_week, symbol, date
                      )
                      union all
                      (
                          with filtered_base_tickers as
                                   (
                                       select contract_name as symbol, exchange_canonical
                                       from {{ ref('ticker_options_monitored') }}
                                       join {{ ref('base_tickers') }} using (symbol)
                                       where exchange_canonical is not null
                                   ),
                               time_series_1w as
                                   (
                                       SELECT distinct exchange_canonical, dd as date
                                       FROM generate_series(
                                           date_trunc('week', now() - interval '5 year' - interval '1 week')::timestamp,
                                           date_trunc('week', now())::timestamp,
                                           interval '1 week') dd
                                       join filtered_base_tickers on true
                                   )
                          select symbol,
                                 date,
                                 null::double precision as open,
                                 null::double precision as high,
                                 null::double precision as low,
                                 null::double precision as close,
                                 null::double precision as adjusted_close,
                                 null::double precision as volume,
                                 1                      as priority,
                                 null                   as updated_at
                          from filtered_base_tickers
                                   join time_series_1w using (exchange_canonical)
                      )
                      union all
                      (
                          with filtered_base_tickers as
                                   (
                                       select contract_name as symbol, country_name
                                       from {{ ref('ticker_options_monitored') }}
                                       join {{ ref('base_tickers') }} using (symbol)
                                       where exchange_canonical is null
                                         and (country_name in ('USA') or country_name is null)
                                   ),
                               time_series_1w as
                                   (
                                       SELECT distinct country_name, dd as date
                                       FROM generate_series(
                                                    date_trunc('week', now() - interval '5 year' - interval '1 week')::timestamp,
                                                    date_trunc('week', now())::timestamp,
                                                    interval '1 week') dd
                                                join filtered_base_tickers on true
                                   )
                          select symbol,
                                 date,
                                 null::double precision as open,
                                 null::double precision as high,
                                 null::double precision as low,
                                 null::double precision as close,
                                 null::double precision as adjusted_close,
                                 null::double precision as volume,
                                 1                      as priority,
                                 null                   as updated_at
                          from filtered_base_tickers
                                   join time_series_1w using (country_name)
                      )
                  ) t
             order by t.symbol, t.date, priority
         )
select t2.*
from (
         select DISTINCT ON (
             symbol,
             date
             ) symbol || '_' || date  as id,
               symbol,
               date::timestamp        as datetime,
               coalesce(
                       open,
                       first_value(close)
                       OVER (partition by symbol, grp order by date)
                   )                  as open,
               coalesce(
                       high,
                       first_value(close)
                       OVER (partition by symbol, grp order by date)
                   )                  as high,
               coalesce(
                       low,
                       first_value(close)
                       OVER (partition by symbol, grp order by date)
                   )                  as low,
               coalesce(
                       close,
                       first_value(close)
                       OVER (partition by symbol, grp order by date)
                   )                  as close,
               coalesce(
                       adjusted_close,
                       first_value(adjusted_close)
                       OVER (partition by symbol, grp order by date)
                   )                  as adjusted_close,
               coalesce(
                       updated_at,
                       first_value(updated_at)
                       OVER (partition by symbol, grp order by date)
                   )                  as updated_at,
               coalesce(volume, 0.0)  as volume
         from (
                  select combined_daily_prices.*,
                         sum(case when close is not null then 1 end)
                         over (partition by combined_daily_prices.symbol order by date) as grp
                  from combined_daily_prices
              ) t
         order by symbol, date
     ) t2
{% if is_incremental() %}
         left join {{ this }} old_data using (symbol, datetime)
{% endif %}
where t2.adjusted_close is not null
{% if is_incremental() %}
  and (old_data.symbol is null -- no old data
   or (t2.updated_at is not null and old_data.updated_at is null) -- old data is null and new is not
   or t2.updated_at > old_data.updated_at) -- new data is newer than the old one
{% endif %}

-- OK created incremental model historical_prices_aggregated_1w  SELECT 2980681 in 90.61s
-- OK created incremental model historical_prices_aggregated_1w  SELECT 2980681 in 156.91s
