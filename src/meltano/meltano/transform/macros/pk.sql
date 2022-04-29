{% macro pk(columns) %}

{% set sql %}
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT constraint_schema, constraint_name
            FROM information_schema.table_constraints
            WHERE constraint_schema =  '{{ this.schema }}'
                AND constraint_name = 'pk_{{ this.name }}'
        )
        THEN
            ALTER TABLE {{ this }}
                ADD CONSTRAINT pk_{{ this.name }} PRIMARY KEY ({{columns}});
        END IF;
    END$$;

{% endset %}

{{ return(after_commit(sql)) }}

{% endmacro %}
