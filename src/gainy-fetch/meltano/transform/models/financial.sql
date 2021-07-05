{%- set j = read_json_column('financials', 'fundamentals') -%}

select
code as symbol,
 {% for k in j.keys() %}
    financials -> '{{k}}' as {{k}}
    {% if not loop.last %}
        ,
    {% endif %}
 {% endfor %}
from fundamentals
