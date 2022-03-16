{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
    ]
  )
}}

{% if is_incremental() %}
with max_updated_at as (select max(updated_on) as max_date from {{ this }})
{% endif %}
select id::varchar,
       name::varchar as title,
       post_summary::varchar,
       main_image::varchar,
       category_link::varchar,
       category_name::varchar,
       published_on::timestamp,
       rate_rating::double precision,
       rate_votes::int,
       ('https://www.gainy.app/blog/' || slug || '?app')::varchar as url,
       updated_on::timestamp
from {{ source('website', 'blogs') }}
{% if is_incremental() %}
left join max_updated_at on true
where max_updated_at.max_date is null or updated_on::timestamp > max_updated_at.max_date
{% endif %}

