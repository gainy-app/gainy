{{
  config(
    materialized = "view",
  )
}}

select collection_id,
       interest_id,
       sum(weight * (sim_dif + 1) / 2) * 2 - 1 as sim_dif
from {{ ref('collections') }}
         join {{ ref('collection_ticker_actual_weights') }}
              on collection_ticker_actual_weights.collection_id = collections.id
         join {{ ref('ticker_interests_continuous') }} using (symbol)
where collections.enabled = '1'
group by collection_id, interest_id
