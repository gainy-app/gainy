{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('contract_name, date'),
      index(this, 'id', true),
    ],
  )
}}

select (contract_name || '_' || to_timestamp(t / 1000)::date)::varchar as id,
       contract_name,
       c                                                               as close,
       h                                                               as high,
       l                                                               as low,
       n                                                               as tx_cnt,                       -- The number of transactions in the aggregate window.
       o                                                               as open,
       to_timestamp(t / 1000)::date                                    as date,
       v                                                               as volume,
       vw                                                              as volume_weighted_average_price -- The tick's volume weighted average price.
from {{ source('polygon', 'polygon_options_historical_prices') }}
join {{ ref('ticker_options_monitored') }} using (contract_name)