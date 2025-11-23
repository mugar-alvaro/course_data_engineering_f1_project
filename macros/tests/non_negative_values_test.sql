{% test non_negative(model, column_name) %}
    -- Falla si hay valores negativos
    select *
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}