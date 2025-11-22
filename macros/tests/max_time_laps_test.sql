{% test max_ms(model, column_name, max_ms) %}
    select *
    from {{ model }}
    where {{ column_name }} > {{ max_ms }}
{% endtest %}