{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
    ]
  )
}}

{% if is_incremental() %}
with max_updated_at as (select max(updated_on) as max_date from {{ this }})
{% endif %}
select blogs.id::varchar,
       blogs.name::varchar                               as title,
       post_summary::varchar,
       main_image::varchar,
       category_link::varchar,
       category_name::varchar,
       published_on::timestamp,
       rate_rating::double precision,
       rate_votes::float,
       ('https://www.gainy.app/blog/' || slug || '?app') as url,
       updated_on::timestamp,
       coalesce(priority::int, 0)                        as priority
from {{ source('website', 'blogs') }}
         left join {{ source('gainy', 'blog_article_priority') }} using (id)

{% if is_incremental() %}
left join max_updated_at on true
{% endif %}

where updated_at > (select max(updated_at) from {{ source('website', 'blogs') }}) - interval '1 minute'

{% if is_incremental() %}
  and (max_updated_at.max_date is null or updated_on::timestamp > max_updated_at.max_date)
{% endif %}
