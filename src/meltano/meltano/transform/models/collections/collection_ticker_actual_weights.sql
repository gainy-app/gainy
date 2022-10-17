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
         select distinct on (
             collection_uniq_id, symbol
             ) profile_id,
               collection_id,
               collection_uniq_id,
               symbol,
               date,
               weight,
               price,
               collection_uniq_id || '_' || symbol as id,
               updated_at
         from {{ ref('collection_ticker_weights') }}
         order by collection_uniq_id, symbol, date desc
     ) t
{% if is_incremental() %}
         left join {{ this }} old_data using (collection_uniq_id, symbol)
where old_data is null or old_data.date < t.date
{% endif %}
