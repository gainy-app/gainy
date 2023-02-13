{{
  config(
    materialized = "view",
  )
}}

with collection_category_tags as (
    select collection_categories.collection_id,
           categories.name as tag,
           case
               when sim_dif > 0.25 then 2
           else 4
           end             as priority
    from {{ ref('collection_categories') }}
             join {{ ref('categories') }} on collection_categories.category_id = categories.id
    where sim_dif > 0
),
collection_industry_tags as (
    select
        ci.collection_id,
        gainy_industries.name as tag,
        case
            when industry_grade >= 0.6 then 1
            when industry_grade >= 0.2 then 2
            when industry_grade >= 0.02 then 3
            else 4
        end as priority
        from {{ ref('collection_industries') }} ci
            join {{ ref('gainy_industries') }}
                on ci.industry_id = gainy_industries.id
),
collection_tags as (
    select
        collection_id,
        array_agg(tag) filter (where priority = 1) as tag_1,
        array_agg(tag) filter (where priority = 2) as tag_2,
        array_agg(tag) filter (where priority = 3) as tag_3
    from (
        select collection_id, tag, priority
        from collection_industry_tags
        union
        select collection_id, tag, priority
        from collection_category_tags
    ) tags_flatten
    group by
        collection_id
)
select c.id, c."name", c.description, tag_1, tag_2, tag_3
from {{ ref('profile_collections') }} c
         left join collection_tags ct
                   on c.id = ct.collection_id
where c.enabled = '1'
  and c.personalized = '0'
