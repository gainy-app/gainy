{%- set j = read_json_column('highlights', 'fundamentals') -%}

select
code as symbol,
 {% for k in j.keys() %}
    highlights -> '{{k}}' as {{k}}
    {% if not loop.last %}
        ,
    {% endif %}
 {% endfor %}
from fundamentals
