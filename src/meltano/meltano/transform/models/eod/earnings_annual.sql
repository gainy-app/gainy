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
               (json_each((earnings -> 'Annual')::json)).*,
               case
                   when is_date(updatedat)
                       then updatedat::timestamp
                   else _sdc_batched_at
                   end as updated_at
        from {{ source('eod', 'eod_fundamentals') }}
    )
    select symbol,
           key::date                        as date,
           (value ->> 'epsActual')::numeric as eps_actual,
           updated_at
    from expanded
    where key != '0000-00-00'
