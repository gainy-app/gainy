{% macro read_json_column(column, table) %}

{% set get_general_query %}
select distinct
    {{column}}
from {{table}}
order by 1
{% endset %}

{% set results = run_query(get_general_query) %}

{{ log(results, info=True) }}

{% if execute %}
{% set results_list = fromjson(results.columns[0].values()[0]) %}
{% else %}
{% set results_list = {} %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
