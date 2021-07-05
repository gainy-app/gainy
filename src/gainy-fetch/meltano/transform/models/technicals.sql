{%- set j = read_json_column('technicals', 'fundamentals') -%}

select
code as symbol,
 {% for k in j.keys() %}
    technicals -> '{{k}}' as {{k}}
    {% if not loop.last %}
        ,
    {% endif %}
 {% endfor %}
from fundamentals
