{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('collection_id, category_id'),
      index('id', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


select collection_id,
       category_id,
       sum(weight * sim_dif)                 as sim_dif,
       (collection_id || '_' || category_id) as id,
       now()                                 as updated_at
from {{ ref('collections') }}
         join {{ ref('collection_ticker_actual_weights') }}
              on collection_ticker_actual_weights.collection_id = collections.id
         join {{ ref('ticker_categories_continuous') }} using (symbol)
where collections.enabled = '1'
group by collection_id, category_id
having sum(weight * sim_dif) > 0
