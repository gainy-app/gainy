{{
  config(
    materialized = "table",
    sort = "created_at",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with earnings_history as (select * from {{ ref('earnings_history') }}),
     tickers as (select * from {{ ref('tickers') }}),
     upcoming_reports as
         (
             select symbol,
                    min(report_date) as date
             from earnings_history
             where eps_actual is null
             group by symbol
         ),
     recent_reports as
         (
             select symbol, max(report_date) as recent_report
             from earnings_history
             where eps_difference is not null
             group by symbol
         ),
     distinct_reports as
         (
             select report_date,
                    symbol,
                    eps_difference,
                    max(date)                                                         as date,
                    ROW_NUMBER() over (partition by symbol order by report_date desc) as r
             from earnings_history
             where eps_difference is not null
               and report_date < now()
             group by report_date, symbol, eps_difference
             order by date desc
         )

/* IPO'ed during last 3 months */
select symbol,
       'Went public on ' || ipo_date as highlight,
       now()                         as created_at
from tickers
where ipo_date > now() - interval '3 months'

union

/* Upcoming earnings. Bit expectations x/4 times.  */
select distinct_reports.symbol,
       case
           when max(upcoming_reports.date) is not null
               then 'Reports earnings on ' || max(upcoming_reports.date) || '. '
           else '' end ||
       'Beaten analysts expectations ' ||
       count(1) || ' of 4 last quarters.' as highlight,
       now()                              as created_at
from distinct_reports
         left join recent_reports on recent_reports.symbol = distinct_reports.symbol
         left join upcoming_reports on distinct_reports.symbol = upcoming_reports.symbol and
                                       upcoming_reports.date <= now() + interval '2 weeks' and
                                       upcoming_reports.date > now()
where eps_difference > 0
  and r <= 4
group by distinct_reports.symbol
