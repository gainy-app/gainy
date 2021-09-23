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

with upcoming_reports as
         (
             select symbol,
                    min(report_date) as upcoming_report
             from {{ ref('earnings_history') }}
             where eps_actual is null
             group by symbol
         ),
     recent_reports as
         (
             select symbol, max(report_date) as recent_report
             from {{ ref('earnings_history') }}
             where eps_difference is not null
             group by symbol),
     distinct_reports as
         (
             select report_date,
                    symbol,
                    eps_difference,
                    max(date)                                                         as date,
                    ROW_NUMBER() over (partition by symbol order by report_date desc) as r
             from {{ ref('earnings_history') }}
             where eps_difference is not null
               and report_date < now()
             group by report_date, symbol, eps_difference
             order by date desc
         )
    (
        /* IPO'ed during last 3 months */
        select symbol,
               symbol || ' went public on ' || ipo_date as highlight,
               now()                                    as created_at
        from {{ ref('tickers') }}
        where ipo_date > now() - interval '3 months'
    )
union
(
    /* Upcoming earnings. Bit expectations x/4 times.  */
    select distinct_reports.symbol,
           distinct_reports.symbol || ' reports earnings on ' || max(upcoming_report) ||
           '. Beaten analysts expectations ' ||
           count(1) || ' of 4 last quarters.' as highlight,
           now()                              as created_at
    from distinct_reports
             left join recent_reports on recent_reports.symbol = distinct_reports.symbol
             left join upcoming_reports on distinct_reports.symbol = upcoming_reports.symbol
    where eps_difference > 0
      and r <= 4
      and upcoming_report <= now() + interval '2 weeks'
      and upcoming_report > now()
    group by distinct_reports.symbol
)




