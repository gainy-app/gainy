{% macro fk2(from_column, to_model, to_column) %}

{% set sql %}
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT constraint_schema, constraint_name
            FROM information_schema.table_constraints
            WHERE constraint_schema =  '{{ this.schema }}'
                AND constraint_name = 'fk_{{ from_column }}_{{ to_model.table }}_{{ to_column }}'
        )
        THEN
            ALTER TABLE {{ this }}
                ADD CONSTRAINT fk_{{ from_column }}_{{ to_model.table }}_{{ to_column }} FOREIGN KEY ({{from_column}}) REFERENCES {{to_model}} ({{to_column}}) on update cascade on delete cascade;
        END IF;
    END$$;

{% endset %}

{{ return(after_commit(sql)) }}

{% endmacro %}
