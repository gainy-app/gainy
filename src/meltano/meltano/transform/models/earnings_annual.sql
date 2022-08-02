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
        select code as symbol,
               (json_each((earnings -> 'Annual')::json)).*,
               updatedat::date as updated_at
        from {{ source('eod', 'eod_fundamentals') }} f
                 inner join {{  ref('tickers') }} as t on f.code = t.symbol
    )
    select symbol,
           key::date                       as date,
           (value ->> 'epsActual')::float4 as eps_actual,
           updated_at
    from expanded
    where key != '0000-00-00'
