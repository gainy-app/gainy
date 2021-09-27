{% macro get_index_name(model, idx_name) %}
    {{ model.schema ~ '_' ~ model.table ~ '_' ~ idx_name ~ '_' ~ run_started_at.strftime('%Y%m%d_%H%M%S') ~ ' ON ' ~ model }}
{% endmacro %}