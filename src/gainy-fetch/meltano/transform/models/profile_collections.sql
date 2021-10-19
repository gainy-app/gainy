{{
  config(
    materialized = "view"
  )
}}

{% set min_collection_size = 2 %}

with profile_collections as (
    select
        null::integer as profile_id,
        id,
        name,
        description,
        image_url,
        enabled,
        size
    from {{ ref('collections') }}
        where
            personalized = '0'
    union
    select
        csp.profile_id,
        c.id,
        c.name,
        c.description,
        c.image_url,
        enabled,
        csp.size
    from {{ ref('collections') }} c
        join {{ source('app', 'personalized_collection_sizes') }} csp
            on c.id = csp.collection_id
        where
            c.personalized = '1'
)
select
    profile_id,
    id,
    name,
    description,
    image_url,
    case
        when enabled = '0' then '0'
        when size is null or size < {{ min_collection_size }} then '0'
        else '1'
    end as enabled,
    coalesce(size, 0) as size
from profile_collections