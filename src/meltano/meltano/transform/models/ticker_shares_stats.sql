{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

with tickers as (select * from {{ ref('tickers') }} where type != 'crypto'),
     eod_fundamentals as (select * from {{ source('eod', 'eod_fundamentals') }})
select f.code                                               as symbol,
       (sharesstats ->> 'ShortRatio')::numeric              as short_ratio,
       (sharesstats ->> 'SharesFloat')::bigint              as shares_float,
       (sharesstats ->> 'SharesShort')::bigint              as shares_short,
       (sharesstats ->> 'PercentInsiders')::numeric         as percent_insiders,
       (sharesstats ->> 'SharesOutstanding')::bigint        as shares_outstanding,
       (sharesstats ->> 'ShortPercentFloat')::numeric       as short_percent_float,
       (sharesstats ->> 'PercentInstitutions')::numeric     as percent_institutions,
       (sharesstats ->> 'SharesShortPriorMonth')::numeric   as shares_short_prior_month,
       (sharesstats ->> 'ShortPercentOutstanding')::numeric as short_percent_outstanding,
       case
           when is_date(updatedat)
               then updatedat::timestamp
           else _sdc_batched_at
           end                                            as updated_at
from eod_fundamentals f
         JOIN tickers t ON t.symbol = f.code