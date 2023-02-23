{{
  config(
    materialized = "view",
  )
}}

select collection_id,
       category_id,
       sum(weight * sim_dif) as sim_dif
from {{ ref('collections') }}
         join {{ ref('collection_ticker_actual_weights') }}
              on collection_ticker_actual_weights.collection_id = collections.id
         join {{ ref('ticker_categories_continuous') }} using (symbol)
where collections.enabled = '1'
group by collection_id, category_id
having sum(weight * sim_dif) > 0
