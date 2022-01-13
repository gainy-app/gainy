{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      index(this, 'symbol', false),
      'delete from {{this}} where created_at < (select max(created_at) from {{this}})',
    ]
  )
}}

with earnings_history as (select * from {{ ref('earnings_history') }}),
     tickers as (select * from {{ ref('tickers') }}),
     highlights as (select * from {{ ref('highlights') }}),
     upcoming_report_date as
         (
             select symbol,
                    min(report_date) as date
             from earnings_history
             where eps_actual is null
               and report_date BETWEEN now() and now() + interval '2 weeks'
             group by symbol
         )

/* IPO'ed during last 3 months */
select concat(symbol, '_ipo')::varchar as id,
       symbol,
       'Went public on ' || ipo_date as highlight,
       now()                         as created_at
from tickers
where ipo_date > now() - interval '3 months'

union

/* Upcoming earnings. Bit expectations x/4 times.  */
select concat(tickers.symbol, '_upcoming_earnings')::varchar as id,
       tickers.symbol,
       case
           when upcoming_report_date.date is not null
               then 'Reports earnings on ' || upcoming_report_date.date || '. '
           else '' end ||
       'Beaten analysts expectations ' || highlights.beaten_quarterly_eps_estimation_count_ttm ||
       ' of 4 last quarters.' as highlight,
       now()                  as created_at
from tickers
         left join highlights on highlights.symbol = tickers.symbol
         left join upcoming_report_date on tickers.symbol = upcoming_report_date.symbol
