{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol, date'),
    ]
  )
}}


    with expanded as (
        select code    as symbol,
               (json_each((earnings -> 'History')::json)).*,
               case
                   when is_date(updatedat)
                       then updatedat::timestamp
                   else _sdc_batched_at
                   end as updated_at
        from {{ source('eod', 'eod_fundamentals') }}
    )
    select symbol,
           key::date                              as date,
           (value ->> 'currency')::text           as currency,
           (value ->> 'epsActual')::numeric       as eps_actual,
           (value ->> 'reportDate')::date         as report_date,
           (value ->> 'epsEstimate')::numeric     as eps_estimate,
           (value ->> 'epsDifference')::numeric   as eps_difference,
           (value ->> 'surprisePercent')::numeric as surprise_percent,
           (value ->> 'beforeAfterMarket')::text  as before_after_market,
           updated_at
    from expanded
    where key != '0000-00-00'
