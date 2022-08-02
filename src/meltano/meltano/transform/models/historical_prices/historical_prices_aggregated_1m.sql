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


select t.code || '_' || t.date_month as id,
       t.code                        as symbol,
       t.date_month                  as datetime,
       hp_open.open,
       t.high,
       t.low,
       hp_close.close,
       hp_close.adjusted_close,
       hp_close.updated_at,
       t.volume
from (
         select code,
                date_month,
                mode() within group ( order by date )      as open_date,
                mode() within group ( order by date desc ) as close_date,
                max(high)                                  as high,
                min(low)                                   as low,
                sum(volume)                                as volume
         from {{ ref('historical_prices') }}
         group by code, date_month
     ) t
         join {{ ref('historical_prices') }} hp_open on hp_open.code = t.code and hp_open.date = t.open_date
         join {{ ref('historical_prices') }} hp_close on hp_close.code = t.code and hp_close.date = t.close_date
{% if is_incremental() %}
         left join {{ this }} old_data on old_data.symbol = t.code and old_data.datetime = t.date_month
where old_data.symbol is null -- no old data
   or (hp_close.updated_at is not null and old_data.updated_at is null) -- old data is null and new is not
   or hp_close.updated_at > old_data.updated_at -- new data is newer than the old one
{% endif %}

-- OK created incremental model historical_prices_aggregated_1m  SELECT 1914573 in 225.34s
-- OK created incremental model historical_prices_aggregated_1m  SELECT 1914573 in 367.38s

-- Hash Join  (cost=3753167.56..9119469.03 rows=832881 width=61) (actual time=41939.432..100542.734 rows=1914573 loops=1)
--   Hash Cond: (((hp_open.code)::text = (hp_close.code)::text) AND (t.close_date = hp_close.date))
--   ->  Hash Join  (cost=1857253.06..6910861.97 rows=1697250 width=54) (actual time=19067.470..74758.818 rows=1914573 loops=1)
--         Hash Cond: (((t.code)::text = (hp_open.code)::text) AND (t.open_date = hp_open.date))
--         ->  Subquery Scan on t  (cost=0.56..4731262.96 rows=3958954 width=45) (actual time=0.036..41364.373 rows=1914573 loops=1)
--               ->  GroupAggregate  (cost=0.56..4691673.42 rows=3958954 width=45) (actual time=0.034..41139.280 rows=1914573 loops=1)
--                     Group Key: historical_prices.code, historical_prices.date_month
--                     ->  Index Scan using code__date_month__date on historical_prices  (cost=0.56..3939472.16 rows=39589540 width=41) (actual time=0.012..23662.185 rows=39580763 loops=1)
--         ->  Hash  (cost=1031439.40..1031439.40 rows=39589540 width=17) (actual time=19065.908..19065.909 rows=39580763 loops=1)
--               Buckets: 65536  Batches: 1024  Memory Usage: 2444kB
--               ->  Seq Scan on historical_prices hp_open  (cost=0.00..1031439.40 rows=39589540 width=17) (actual time=0.014..7827.632 rows=39580763 loops=1)
--   ->  Hash  (cost=1031439.40..1031439.40 rows=39589540 width=25) (actual time=19244.402..19244.403 rows=39580763 loops=1)
--         Buckets: 65536  Batches: 1024  Memory Usage: 2747kB
--         ->  Seq Scan on historical_prices hp_close  (cost=0.00..1031439.40 rows=39589540 width=25) (actual time=0.009..7501.225 rows=39580763 loops=1)
-- Planning Time: 1.152 ms
-- Execution Time: 100626.144 ms
