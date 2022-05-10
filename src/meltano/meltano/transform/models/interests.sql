{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}

SELECT id::int,
       name,
       icon_url,
       enabled,
       "order"::int as sort_order,
       trim("sub 1") as sub_1_name,
       trim("sub 2") as sub_2_name,
       trim("sub 3") as sub_3_name,
       trim("sub 4") as sub_4_name,
       trim("sub 5") as sub_5_name,
       trim("sub 6") as sub_6_name,
       trim("sub 7") as sub_7_name,
       trim("sub 8") as sub_8_name,
       trim("sub 9") as sub_9_name,
       trim("sub 10") as sub_10_name,
       trim("sub 11") as sub_11_name,
       trim("sub 12") as sub_12_name,
       trim("sub 13") as sub_13_name,
       trim("sub 14") as sub_14_name,
       trim("sub 15") as sub_15_name,
       "ttf 1" as ttf_1_name,
       "ttf 2" as ttf_2_name,
       "ttf 3" as ttf_3_name,
       "ttf 4" as ttf_4_name,
       "ttf 5" as ttf_5_name,
       "ttf 6" as ttf_6_name,
       "ttf 7" as ttf_7_name,
       "ttf 8" as ttf_8_name,
       "ttf 9" as ttf_9_name,
       "ttf 10" as ttf_10_name,
       now()::timestamp as updated_at
FROM {{ source('gainy', 'gainy_interests') }}
where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ source('gainy', 'gainy_interests') }}) - interval '1 minute'
