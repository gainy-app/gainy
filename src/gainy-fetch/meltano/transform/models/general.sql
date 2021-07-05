{%- set j = read_json_column('general', 'fundamentals') -%}

select
 {% for k in j.keys() %}
    general -> '{{k}}' as {{k}}
    {% if not loop.last %}
        ,
    {% endif %}
 {% endfor %}
from fundamentals
