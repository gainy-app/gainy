{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

with upcoming_reports as
         (
             select symbol,
                    min(report_date) as date
             from {{ ref('earnings_history') }}
             where eps_actual is null
             group by symbol
         )
select upcoming_reports.symbol,
       date,
       'earnings_report'                                                 as type,
       upcoming_reports.symbol || ' reports earnings on ' || date || '.' as description,
       now()                                                             as created_at
from upcoming_reports
         join tickers on tickers.symbol = upcoming_reports.symbol
where date <= now() + interval '2 weeks'
  and date > now()




