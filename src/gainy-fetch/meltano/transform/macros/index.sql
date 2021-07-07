{% macro index(this, column, unique) %}

create {% if unique %} unique {% endif %} index if not exists "{{ this.name }}__index_on_{{ column }}" on {{ this }} ("{{ column }}");

{% if unique %}
ALTER TABLE {{ this.name }} DROP CONSTRAINT IF EXISTS {{ this.name }}_unique_{{ column }};
ALTER TABLE {{ this.name }} ADD CONSTRAINT {{ this.name }}_unique_{{ column }} UNIQUE ({{column}});
{% endif %}


{% endmacro %}
