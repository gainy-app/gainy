{%- set j = expand_json_column('highlights', 'fundamentals') -%}

select
code as symbol,
 {{  j  }}
from fundamentals
