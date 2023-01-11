{% macro index(columns, unique) %}

{% set sql %}
    {% set columns = [columns] if columns is string else columns %}

    {% if unique %}
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT constraint_schema, constraint_name
                FROM information_schema.table_constraints
                WHERE constraint_schema = '{{ this.schema }}'
                    AND constraint_name = '{{ this.name }}_unique_{{ columns|join('_') }}'
            )
            THEN
                ALTER TABLE {{ this }} ADD CONSTRAINT {{ this.name }}_unique_{{ columns|join('_') }} UNIQUE ({{columns|join(', ')}});
            END IF;
        END$$;
    {% else %}
        CREATE INDEX IF NOT EXISTS {{ this.name }}_index_{{ columns|join('_') }} ON {{ this.schema }}.{{ this.name }} ({{columns|join(', ')}});
    {% endif %}
{% endset %}


{{ return(after_commit(sql)) }}


{% endmacro %}
