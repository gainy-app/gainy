{% macro expand_json_column(column, table) %}

{%- set j = read_json_column(column, table) -%}

{% set result %}
    {% for k in j.keys() %}
        {{column}} ->> '{{k}}' as {{k}}{% if not loop.last %},{% endif %}
    {% endfor %}
{% endset %}

{{ return(result) }}

{% endmacro %}
