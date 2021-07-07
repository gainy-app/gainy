{%- set j = expand_json_column('general', 'fundamentals') -%}

select
 {{ j }}
from fundamentals
