{{
  config(
    enabled=false
  )
}}

{%- set j = expand_json_column('financials', 'fundamentals') -%}

select
 code as symbol,
 {{ j }}
from {{ source('eod', 'fundamentals') }}
