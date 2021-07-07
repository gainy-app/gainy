{%- set j = read_json_column('valuation', 'fundamentals') -%}

select
    code as symbol,
 {{ j }}
from fundamentals
