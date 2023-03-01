{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

select *
from (
         select code                                           as symbol,
                (sharesstats ->> 'SharesFloat')::numeric       as shares_float,
                (sharesstats ->> 'SharesOutstanding')::numeric as shares_outstanding,
                case
                    when is_date(updatedat)
                        then updatedat::timestamp
                    else _sdc_batched_at
                    end                                        as updated_at
         from {{ source('eod', 'eod_fundamentals') }}
     ) t
where shares_float is not null
   or shares_outstanding is not null
