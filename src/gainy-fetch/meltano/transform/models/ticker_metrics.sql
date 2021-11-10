{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with highlights as (select * from {{ ref('highlights') }}),
     valuation as (select * from {{ ref('valuation') }}),
     technicals as (select * from {{ ref('technicals') }}),
     ticker_shares_stats as (select * from {{ ref('ticker_shares_stats') }}),
     marked_prices as
         (
             select distinct on (hp.code, hp.period) *
             from (
                      select *,
                             case
                                 when hp."date" <= hp.cur_date - interval '3 month' then '3m'
                                 when hp."date" <= hp.cur_date - interval '1 month' then '1m'
                                 when hp."date" <= hp.cur_date - interval '10 days' then '10d'
                                 when hp."date" <= hp.cur_date then '0d'
                                 end as period
                      from (
                               select code,
                                      "date"::date,
                                      adjusted_close::numeric                                                 as price,
                                      volume,
                                      first_value("date"::date) over (partition by code order by "date" desc) as cur_date
                               from historical_prices
                           ) hp
                  ) hp
             where period is not null
             order by hp.code, hp.period, hp.date desc
         ),
     avg_volume_10d as
         (
             select hp.code,
                    avg(hp.volume) as value
             from historical_prices hp
                      join marked_prices mp on mp.code = hp.code and mp.period = '10d'
             where hp.date > mp.date
             group by hp.code
         ),
     avg_volume_90d as
         (
             select hp.code,
                    avg(hp.volume) as value
             from historical_prices hp
                      join marked_prices mp on mp.code = hp.code and mp.period = '3m'
             where hp.date > mp.date
             group by hp.code
         ),
     today_price as (select * from marked_prices mp where period = '0d')
select DISTINCT ON
    (h.symbol) h.symbol,

    /* Selected */
               h.market_capitalization::bigint,
               case
                   when today_price.price > 0
                       then (select mp.price from marked_prices mp where code = h.symbol and period = '1m') /
                            today_price.price
                   end::double precision                                  as month_price_change,
               h.quarterly_revenue_growth_yoy::double precision,
               valuation.enterprise_value_revenue::double precision       as enterprise_value_to_sales,
               h.profit_margin::double precision,
    /* Trading Statistics */
               avg_volume_10d.value::double precision                     as avg_volume_10d,
               ticker_shares_stats.short_percent_outstanding::double precision,
               ticker_shares_stats.shares_outstanding::bigint,
               avg_volume_90d.value::double precision                     as avg_volume_90d,
               ticker_shares_stats.shares_float::bigint,
               ticker_shares_stats.short_ratio::double precision,
               technicals.beta::double precision
from highlights h
         left join valuation on h.symbol = valuation.symbol
         left join technicals on h.symbol = technicals.symbol
         left join ticker_shares_stats on h.symbol = ticker_shares_stats.symbol
         left join avg_volume_10d on h.symbol = avg_volume_10d.code
         left join avg_volume_90d on h.symbol = avg_volume_90d.code
         left join today_price on h.symbol = today_price.code
