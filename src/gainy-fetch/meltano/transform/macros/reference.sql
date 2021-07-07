{% macro fk_constraint(this, from, to_table, to_column) %}

ALTER TABLE {{ this.name }}
    ADD CONSTRAINT fk_{{ from }}_{{ to_table }}_{{ to_column }} FOREIGN KEY ({{from}}) REFERENCES {{to_table}} ({{to_column}});

{% endmacro %}
