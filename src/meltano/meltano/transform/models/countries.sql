{{
  config(
    materialized = "view",
  )
}}


select name,
       "alpha-2" as alpha2,
       "alpha-3" as alpha3,
       flag_w40_url
from {{ source('gainy', 'gainy_countries') }}
