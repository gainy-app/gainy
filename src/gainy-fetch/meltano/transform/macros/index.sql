{% macro index(this, column, unique) %}

{% set sql %}
    {% if unique %}
        ALTER TABLE {{ this.schema }}.{{ this.name }} DROP CONSTRAINT IF EXISTS {{ this.name }}_unique_{{ column }};
        ALTER TABLE {{ this.schema }}.{{ this.name }} ADD CONSTRAINT {{ this.name }}_unique_{{ column }} UNIQUE ({{column}});
    {% else %}
        DROP INDEX IF EXISTS {{ this.schema }}.{{ this.name }}_index_{{ column }};
        CREATE INDEX {{ this.name }}_index_{{ column }} ON {{ this.schema }}.{{ this.name }} ({{column}});
    {% endif %}
{% endset %}


{{ return(after_commit(sql)) }}


{% endmacro %}
