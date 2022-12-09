{{
  config(
    materialized = "table",
    post_hook=[
      pk('collection_id'),
      index('rank', true),
    ]
  )
}}


with unique_collection_stats as
         (
             select collection_id::int,
                    max(clicks_count)::int as clicks_count
             from {{ source('gainy', 'stats_ttf_clicks') }}
                      join (
                               select max(updated_at) as updated_at
                               from {{ source('gainy', 'stats_ttf_clicks') }}
                           ) t using (updated_at)
             group by collection_id
         )
select collection_id,
       row_number() over (order by clicks_count desc) as rank
from unique_collection_stats
