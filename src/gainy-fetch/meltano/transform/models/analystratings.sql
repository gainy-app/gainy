{%- set j = read_json_column('analystratings', 'fundamentals') -%}

select
 code as symbol,
 {% for k in j.keys() %}
    analystratings -> '{{k}}' as {{k}}{% if not loop.last %},{% endif %}
 {% endfor %}
from fundamentals
