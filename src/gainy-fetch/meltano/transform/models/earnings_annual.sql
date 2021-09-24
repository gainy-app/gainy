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
               (json_each((earnings -> 'Annual')::json)).*
        from fundamentals f
                 inner join {{  ref('tickers') }} as t on f.code = t.symbol
    )
    select symbol,
           key::date                       as date,
           (value ->> 'epsActual')::float4 as eps_actual
    from expanded
    where key != '0000-00-00'
