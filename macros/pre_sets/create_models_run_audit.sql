{% macro f1_create_models_run_audit() %}
  {% set silver_db = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB' %}
  {% set sql %}
    create table if not exists {{ silver_db }}.F1.models_run_audit (
      model_name   varchar,
      row_count    number,
      loaded_at    timestamp_ntz
    );
  {% endset %}

  {% do run_query(sql) %}

  {{ return(sql) }}

{% endmacro %}
