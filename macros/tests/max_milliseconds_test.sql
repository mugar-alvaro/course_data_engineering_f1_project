{% test max_milliseconds_race(model, column_name, max_value) %}
    select
        {{ column_name }} as value
    from {{ model }}
    where {{ column_name }} > {{ max_value }}

{% endtest %}
