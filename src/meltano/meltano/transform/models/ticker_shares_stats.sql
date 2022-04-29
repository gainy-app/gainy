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
select f.code                                             as symbol,
       (sharesstats ->> 'ShortRatio')::float              as short_ratio,
       (sharesstats ->> 'SharesFloat')::float             as shares_float,
       (sharesstats ->> 'SharesShort')::float             as shares_short,
       (sharesstats ->> 'PercentInsiders')::float         as percent_insiders,
       (sharesstats ->> 'SharesOutstanding')::float       as shares_outstanding,
       (sharesstats ->> 'ShortPercentFloat')::float       as short_percent_float,
       (sharesstats ->> 'PercentInstitutions')::float     as percent_institutions,
       (sharesstats ->> 'SharesShortPriorMonth')::float   as shares_short_prior_month,
       (sharesstats ->> 'ShortPercentOutstanding')::float as short_percent_outstanding
from eod_fundamentals f
         JOIN tickers t ON t.symbol = f.code