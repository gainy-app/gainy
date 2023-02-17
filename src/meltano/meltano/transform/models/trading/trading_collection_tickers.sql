{{
  config(
    materialized = "view",
  )
}}


with data as
         (
             select collection_id,
                    collection_ticker_actual_weights.symbol,
                    weight
             from {{ ref('collection_ticker_actual_weights') }}
                      join {{ source('app', 'drivewealth_instruments') }}
                           on collection_ticker_actual_weights.symbol =
                              public.normalize_drivewealth_symbol(drivewealth_instruments.symbol)
             where drivewealth_instruments.status = 'ACTIVE'
         ),
     stats as
         (
             select collection_id,
                    sum(weight) as weight_sum
             from data
             group by collection_id
     )
select collection_id, symbol, weight / weight_sum as weight
from data
         join stats using (collection_id)