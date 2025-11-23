{% test exists_in_model(model, column_name, ref_model, ref_column) %}
select *
from {{ model }}
where {{ column_name }} not in (
    select {{ ref_column }} from {{ ref(ref_model) }}
)
{% endtest %}
