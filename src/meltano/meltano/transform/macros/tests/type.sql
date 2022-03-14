{% test type(model, column_name, type) %}

select '"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"',
       data_type
from information_schema.columns
where '"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"' = '{{model}}'
  and column_name = '{{column_name}}'
  and data_type != '{{type}}'

{% endtest %}