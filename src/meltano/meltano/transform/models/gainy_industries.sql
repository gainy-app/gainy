{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      index(this, 'name', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


with distinct_industries as
    (
        select distinct name
        from (
                 select trim(Industry)::varchar as name
                 from {{ source('gainy', 'crypto_ticker_industries') }}
                 union all
                 select trim(gic_sub_industry)::varchar as name
                 from {{ ref('base_tickers') }}
             ) t
        where name is not null --gic_sub_industry often null from EOD
        order by name
    ),
{% if is_incremental() %}

     max_id as
         (
             select max(id) as max_id
             from {{ this }}
         ),
     raw_data as
         (

             select gainy_industries.id,
                    distinct_industries.name
             from distinct_industries
                      join {{ this }} using (name)

             union all

             select coalesce(max_id.max_id, 0) + (row_number() over ())::integer as id,
                    t.name
             from (
                      select *
                      from distinct_industries
                      left join {{ this }} using (name)
                      where gainy_industries.id is null
             ) t
                      join max_id on true
         )

{% else %}

     raw_data as
         (
             select *,
                    (row_number() over ())::integer as id
             from distinct_industries
         )
{% endif %}

select distinct raw_data.*,
                collections.id as collection_id,
                now()::timestamp as updated_at
from raw_data
         -- The below reference to `collections` table is required for DBT to build correct model dependency graph
         LEFT JOIN {{ ref('collections') }} ON collections.id = 20000 + raw_data.id
where raw_data.name not ilike '%discontinued%'
