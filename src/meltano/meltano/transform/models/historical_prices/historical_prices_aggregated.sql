{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select symbol,
       datetime      as time,
       datetime,
       '3min'        as period,
       open,
       high,
       low,
       close,
       adjusted_close,
       volume
from {{ ref('historical_prices_aggregated_3min') }}

union all

select symbol,
       datetime       as time,
       datetime,
       '15min'        as period,
       open,
       high,
       low,
       close,
       adjusted_close,
       volume
from {{ ref('historical_prices_aggregated_15min') }}

union all

select symbol,
       datetime    as time,
       datetime,
       '1d'        as period,
       open,
       high,
       low,
       close,
       adjusted_close,
       volume
from {{ ref('historical_prices_aggregated_1d') }}

union all

select symbol,
       datetime    as time,
       datetime,
       '1w'        as period,
       open,
       high,
       low,
       close,
       adjusted_close,
       volume
from {{ ref('historical_prices_aggregated_1w') }}

union all

select symbol,
       datetime    as time,
       datetime,
       '1m'        as period,
       open,
       high,
       low,
       close,
       adjusted_close,
       volume
from {{ ref('historical_prices_aggregated_1m') }}
