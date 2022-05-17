{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "interest_id__industry_id" ON {{ this }} (interest_id, industry_id)',
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


with interest_industries_flatten as
    (
        select interests.id as interest_id, sub_1_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_2_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_3_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_4_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_5_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_6_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_7_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_8_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_9_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_10_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_11_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_12_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_13_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_14_name as industry_name from {{ ref('interests') }}
        union
        select interests.id as interest_id, sub_15_name as industry_name from {{ ref('interests') }}
    )

SELECT distinct (interest_id || '_' || gainy_industries.id)::varchar as id,
                interest_id,
                gainy_industries.id as industry_id,
                now() as updated_at
from interest_industries_flatten
         join {{ ref('gainy_industries') }} on gainy_industries.name = interest_industries_flatten.industry_name
