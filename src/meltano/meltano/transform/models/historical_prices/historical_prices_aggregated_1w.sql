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


with combined_daily_prices as
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

-- Subquery Scan on t2  (cost=4395692.77..4396032.77 rows=3980 width=93) (actual time=59127.299..60958.233 rows=2980681 loops=1)
--   Filter: (t2.close IS NOT NULL)
--   Rows Removed by Filter: 3700
--   ->  Unique  (cost=4395692.77..4395992.77 rows=4000 width=101) (actual time=59127.295..60402.678 rows=2984381 loops=1)
--         ->  Sort  (cost=4395692.77..4395792.77 rows=40000 width=101) (actual time=59127.294..59947.883 rows=2984381 loops=1)
--               Sort Key: t.symbol, t.date
--               Sort Method: external merge  Disk: 317952kB
--               ->  WindowAgg  (cost=4389047.73..4390447.73 rows=40000 width=101) (actual time=52285.890..56563.627 rows=2984381 loops=1)
--                     ->  Sort  (cost=4389047.73..4389147.73 rows=40000 width=69) (actual time=52285.870..53106.333 rows=2984381 loops=1)
--                           Sort Key: t.symbol, t.grp, t.date
--                           Sort Method: external merge  Disk: 241936kB
--                           ->  Subquery Scan on t  (cost=4355019.39..4385990.18 rows=40000 width=69) (actual time=45562.326..50083.544 rows=2984381 loops=1)
--                                 ->  WindowAgg  (cost=4355019.39..4385590.18 rows=40000 width=73) (actual time=45562.325..49711.295 rows=2984381 loops=1)
--                                       ->  Subquery Scan on combined_daily_prices  (cost=4355019.39..4384890.18 rows=40000 width=61) (actual time=45562.310..47458.893 rows=2984381 loops=1)
--                                             ->  Unique  (cost=4355019.39..4384490.18 rows=40000 width=65) (actual time=45562.307..46988.828 rows=2984381 loops=1)
--                                                   ->  Sort  (cost=4355019.39..4364842.99 rows=3929439 width=65) (actual time=45562.306..46517.322 rows=2984893 loops=1)
-- "                                                        Sort Key: ""*SELECT* 1"".symbol, ""*SELECT* 1"".date, ""*SELECT* 1"".priority"
--                                                         Sort Method: external merge  Disk: 230312kB
--                                                         ->  Append  (cost=0.57..3602287.06 rows=3929439 width=65) (actual time=2.121..38455.287 rows=2984893 loops=1)
-- "                                                              ->  Subquery Scan on ""*SELECT* 1""  (cost=0.57..3581967.50 rows=3929291 width=65) (actual time=2.120..38180.533 rows=2980685 loops=1)"
--                                                                     ->  Unique  (cost=0.57..3542674.59 rows=3929291 width=69) (actual time=2.119..37647.650 rows=2980685 loops=1)
--                                                                           ->  WindowAgg  (cost=0.57..3466069.82 rows=15320953 width=69) (actual time=2.119..35570.646 rows=15285206 loops=1)
--                                                                                 ->  Index Scan using date_week__symbol__date on historical_prices  (cost=0.57..2968138.85 rows=15320953 width=65) (actual time=2.095..10767.810 rows=15285206 loops=1)
--                                                                                       Index Cond: (date_week >= ((now() - '5 years'::interval) - '7 days'::interval))
--                                                               ->  Hash Join  (cost=426.69..485.13 rows=144 width=92) (actual time=1.881..2.796 rows=4208 loops=1)
--                                                                     Hash Cond: ((filtered_base_tickers_1.exchange_canonical)::text = (filtered_base_tickers.exchange_canonical)::text)
--                                                                     CTE filtered_base_tickers
--                                                                       ->  Nested Loop  (cost=0.29..126.04 rows=12 width=37) (actual time=0.026..0.100 rows=16 loops=1)
--                                                                             ->  Seq Scan on ticker_options_monitored  (cost=0.00..1.16 rows=16 width=64) (actual time=0.006..0.008 rows=16 loops=1)
--                                                                             ->  Index Scan using pk_base_tickers on base_tickers  (cost=0.29..7.80 rows=1 width=11) (actual time=0.005..0.005 rows=1 loops=16)
--                                                                                   Index Cond: ((symbol)::text = (ticker_options_monitored.symbol)::text)
--                                                                                   Filter: (exchange_canonical IS NOT NULL)
--                                                                     ->  HashAggregate  (cost=300.26..324.26 rows=2400 width=40) (actual time=1.754..1.849 rows=526 loops=1)
--                                                                           Group Key: filtered_base_tickers_1.exchange_canonical, dd.dd
--                                                                           ->  Nested Loop  (cost=0.02..240.26 rows=12000 width=40) (actual time=0.061..0.805 rows=4208 loops=1)
--                                                                                 ->  CTE Scan on filtered_base_tickers filtered_base_tickers_1  (cost=0.00..0.24 rows=12 width=32) (actual time=0.001..0.003 rows=16 loops=1)
--                                                                                 ->  Function Scan on generate_series dd  (cost=0.02..10.02 rows=1000 width=8) (actual time=0.004..0.020 rows=263 loops=16)
--                                                                     ->  Hash  (cost=0.24..0.24 rows=12 width=64) (actual time=0.114..0.115 rows=16 loops=1)
--                                                                           Buckets: 1024  Batches: 1  Memory Usage: 9kB
--                                                                           ->  CTE Scan on filtered_base_tickers  (cost=0.00..0.24 rows=12 width=64) (actual time=0.027..0.105 rows=16 loops=1)
--                                                               ->  Hash Join  (cost=176.21..185.75 rows=4 width=92) (actual time=0.110..0.113 rows=0 loops=1)
--                                                                     Hash Cond: (filtered_base_tickers_3.country_name = filtered_base_tickers_2.country_name)
--                                                                     CTE filtered_base_tickers
--                                                                       ->  Nested Loop  (cost=0.29..126.09 rows=2 width=41) (actual time=0.100..0.101 rows=0 loops=1)
--                                                                             ->  Seq Scan on ticker_options_monitored ticker_options_monitored_1  (cost=0.00..1.16 rows=16 width=64) (actual time=0.008..0.009 rows=16 loops=1)
--                                                                             ->  Index Scan using pk_base_tickers on base_tickers base_tickers_1  (cost=0.29..7.81 rows=1 width=15) (actual time=0.005..0.005 rows=0 loops=16)
--                                                                                   Index Cond: ((symbol)::text = (ticker_options_monitored_1.symbol)::text)
--                                                                                   Filter: ((exchange_canonical IS NULL) AND ((country_name = 'USA'::text) OR (country_name IS NULL)))
--                                                                                   Rows Removed by Filter: 1
--                                                                     ->  HashAggregate  (cost=50.06..54.06 rows=400 width=40) (never executed)
--                                                                           Group Key: filtered_base_tickers_3.country_name, dd_1.dd
--                                                                           ->  Nested Loop  (cost=0.02..40.06 rows=2000 width=40) (never executed)
--                                                                                 ->  CTE Scan on filtered_base_tickers filtered_base_tickers_3  (cost=0.00..0.04 rows=2 width=32) (never executed)
--                                                                                 ->  Function Scan on generate_series dd_1  (cost=0.02..10.02 rows=1000 width=8) (never executed)
--                                                                     ->  Hash  (cost=0.04..0.04 rows=2 width=64) (actual time=0.102..0.102 rows=0 loops=1)
--                                                                           Buckets: 1024  Batches: 1  Memory Usage: 8kB
--                                                                           ->  CTE Scan on filtered_base_tickers filtered_base_tickers_2  (cost=0.00..0.04 rows=2 width=64) (actual time=0.101..0.101 rows=0 loops=1)
-- Planning Time: 1.014 ms
-- Execution Time: 61222.620 ms