{% macro f1_flag_time(column_name) %}
    case
        when {{ column_name }} > 600000 then true
        else false
    end
{% endmacro %}
