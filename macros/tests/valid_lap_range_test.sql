{% test valid_lap_range(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 1
   or {{ column_name }} > 120
{% endtest %}
