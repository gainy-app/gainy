{% macro index(this, column, unique) %}

{% set sql %}
    {% if unique %}
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT constraint_schema, constraint_name
                FROM information_schema.table_constraints
                WHERE constraint_schema =  {{ "'" + this.schema + "'" }}
                    AND constraint_name = {{ "'" + this.name + "_unique_" + column + "'" }}
            )
            THEN
                ALTER TABLE {{ this }} ADD CONSTRAINT {{ this.name + "_unique_" + column }} UNIQUE ({{column}});
            END IF;
        END$$;
    {% else %}
        CREATE INDEX IF NOT EXISTS {{ this.name }}_index_{{ column }} ON {{ this.schema }}.{{ this.name }} ({{column}});
    {% endif %}
{% endset %}


{{ return(after_commit(sql)) }}


{% endmacro %}
