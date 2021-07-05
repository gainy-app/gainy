{%- set j = read_json_column('valuation', 'fundamentals') -%}

select
    code as symbol,
 {% for k in j.keys() %}
    valuation -> '{{k}}' as {{k}}
    {% if not loop.last %}
        ,
    {% endif %}
 {% endfor %}
from fundamentals
