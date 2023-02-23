{{
  config(
    materialized = "incremental",
    tags = ["realtime"],
    unique_key = "id",
    post_hook=[
      pk('collection_uniq_id, symbol'),
      index('id', true),
    ]
  )
}}


select t.*
from (
         select profile_id,
                collection_id,
                collection_uniq_id,
                symbol,
                date,
                weight,
                price,
                collection_uniq_id || '_' || symbol as id,
                optimized_at,
                updated_at
         from {{ ref('collection_ticker_weights') }}
                  join (
                           select collection_uniq_id, max(date) as date
                           from {{ ref('collection_ticker_weights') }}
                           group by collection_uniq_id
                       ) t using (collection_uniq_id, date)
     ) t
{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id, symbol)
where old_data.collection_uniq_id is null
   or old_data.date < t.date
   or abs(old_data.weight - t.weight) > 1e-5
{% endif %}
