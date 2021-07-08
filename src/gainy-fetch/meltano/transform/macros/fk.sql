{% macro fk(this, from, to_table, to_column) %}

    {% set sql %}
ALTER TABLE {{ this.name }}
    ADD CONSTRAINT fk_{{ from }}_{{ to_table }}_{{ to_column }} FOREIGN KEY ({{from}}) REFERENCES {{to_table}} ({{to_column}});
    {% endset %}
{{ return(after_commit(sql)) }}

{% endmacro %}
