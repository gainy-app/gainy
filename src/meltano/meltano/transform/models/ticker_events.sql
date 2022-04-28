{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook = [
      pk('symbol, date'),
      index(this, 'id', true),
      'delete from {{this}} where created_at < (select max(created_at) from {{this}})',
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
select concat(upcoming_reports.symbol, '_', date)::varchar as id,
       upcoming_reports.symbol,
       date,
       date::timestamptz                                                 as timestamp,
       'earnings_report'                                                 as type,
       upcoming_reports.symbol || ' reports earnings on ' || date || '.' as description,
       now()                                                             as created_at
from upcoming_reports
         join {{ ref('tickers') }} on tickers.symbol = upcoming_reports.symbol
where date <= now() + interval '2 weeks'
  and date > now()




