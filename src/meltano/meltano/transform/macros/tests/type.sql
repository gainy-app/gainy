{% test type(model, column_name, type) %}

with model_columns as
         (
             select '"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"' as model_name,
                    column_name,
                    data_type
             from information_schema.columns
         )
select *
from (
         (
             values ('{{model}}', '{{column_name}}', '{{type}}')
         )
     ) t(model_name, column_name, data_type)
         left join model_columns using (model_name, column_name)
where model_columns.data_type is null
   or model_columns.data_type != t.data_type

{% endtest %}
