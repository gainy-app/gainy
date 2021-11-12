{% macro fk(from_table, from_column, to_schema, to_table, to_column) %}

{% set sql %}
    ALTER TABLE {{ from_table }}
    ADD CONSTRAINT fk_{{ from_column }}_{{ to_table }}_{{ to_column }} FOREIGN KEY ({{from_column}}) REFERENCES {{to_schema}}.{{to_table}} ({{to_column}});
{% endset %}

{{ return(after_commit(sql)) }}

{% endmacro %}
