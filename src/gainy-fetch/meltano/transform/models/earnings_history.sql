{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', false),
    ]
  )
}}


    with expanded as (
        select code as symbol,
               (json_each((earnings -> 'History')::json)).*
        from fundamentals f
                 inner join {{  ref('tickers') }} as t on f.code = t.symbol
    )
    select symbol,
           key::date                             as date,
           (value ->> 'currency')::text          as currency,
           (value ->> 'epsActual')::float4       as eps_actual,
           (value ->> 'reportDate')::date        as report_date,
           (value ->> 'epsEstimate')::float      as eps_estimate,
           (value ->> 'epsDifference')::float    as eps_difference,
           (value ->> 'surprisePercent')::float  as surprise_percent,
           (value ->> 'beforeAfterMarket')::text as before_after_market
    from expanded
    where key != '0000-00-00'
