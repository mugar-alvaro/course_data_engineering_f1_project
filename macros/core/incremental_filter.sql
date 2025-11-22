{% macro f1_incremental_filter(timestamp_column) %}
    {{ timestamp_column }} > (
      select coalesce(max({{ timestamp_column }}), '1900-01-01'::timestamp)
      from {{ this }}
    )
{% endmacro %}