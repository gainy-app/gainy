{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select *
from {{ ref('collection_tickers_weighted') }}
left join {{ source('app', 'profiles') }} on profiles.id = collection_tickers_weighted.profile_id
where collection_tickers_weighted.profile_id is null
   or (profiles.id is not null and profiles.email not ilike '%test%@gainy.app')
