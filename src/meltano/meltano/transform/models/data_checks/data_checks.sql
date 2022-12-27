{{
  config(
    materialized = "view",
    post_hook=[
      'grant select on {{this}} to datadog',
    ]
  )
}}


select symbol,
       code,
       period,
       message
from {{ ref('data_checks_collections') }}

union all

select symbol,
       code,
       period,
       message
from {{ ref('data_checks_eod_fundamentals') }}

union all

select symbol,
       code,
       period,
       message
from {{ ref('data_checks_eod_historical_prices') }}

union all

select symbol,
       code,
       period,
       message
from {{ ref('data_checks_historical_prices') }}

union all

select symbol,
       code,
       period,
       message
from {{ ref('data_checks_realtime') }}

union all

select symbol,
       code,
       period,
       message
from {{ ref('data_checks_collection_historical_prices') }}

union all

select symbol,
       code,
       period,
       message
from {{ ref('data_checks_drivewealth') }}
