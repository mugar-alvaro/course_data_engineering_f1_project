{% test valid_year_range(model, column_name, min_year=1950, max_year=2050) %}
    select *
    from {{ model }}
    where {{ column_name }} < {{ min_year }}
       or {{ column_name }} > {{ max_year }}
{% endtest %}