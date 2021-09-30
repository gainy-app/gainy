{% test exists(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} != {{ column_name }}

{% endtest %}