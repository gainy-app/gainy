{% macro index(this, column, unique) %}

    {% set sql %}
    {% if unique %}
    ALTER TABLE {{ this.name }} DROP CONSTRAINT IF EXISTS {{ this.name }}_unique_{{ column }};
    ALTER TABLE {{ this.name }} ADD CONSTRAINT {{ this.name }}_unique_{{ column }} UNIQUE ({{column}});
    {% endif %}
{#    create {% if unique %} unique {% endif %} index if not exists "{{ this.name }}__index_on_{{ column }}" on {{ this.name }} ("{{ column }}");#}
    {% endset %}


{{ return(after_commit(sql)) }}


{% endmacro %}
