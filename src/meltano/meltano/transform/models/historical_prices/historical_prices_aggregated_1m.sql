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


-- 1m
-- Execution Time: 111014.110 ms on test
select DISTINCT ON (
    code,
    date_month
    ) code || '_' || date_month                                                                           as id,
      code                                                                                                as symbol,
      date_month                                                                                          as datetime,
      first_value(open)
      OVER (partition by code, date_month order by date rows between current row and unbounded following) as open,
      t.high,
      t.low,
      last_value(close)
      OVER (partition by code, date_month order by date rows between current row and unbounded following) as close,
      last_value(adjusted_close)
      OVER (partition by code, date_month order by date rows between current row and unbounded following) as adjusted_close,
      t.volume
from {{ ref('historical_prices') }}
         join (select code,
                      date_month,
                      max(high)   as high,
                      min(low)    as low,
                      sum(volume) as volume
               from {{ ref('historical_prices') }}
               group by code, date_month) t using (code, date_month)
order by code, date_month, date
