{% macro get_index_name(model, idx_name) %}
    {{ model.schema ~ '_' ~ model.table ~ '_' ~ idx_name ~ ' ON ' ~ model }}
{% endmacro %}