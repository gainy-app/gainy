{%- set j = expand_json_column('technicals', 'fundamentals') -%}

select
code as symbol,
 {{ j }}
from fundamentals
