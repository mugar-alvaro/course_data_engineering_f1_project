{% macro f1_log_model_run() %}
    {% set silver_db = env_var('DBT_ENVIRONMENTS') ~ '_SILVER_DB' %}
    {% set sql %}
        insert into {{ silver_db }}.F1.models_run_audit (
            model_name,
            row_count,
            loaded_at
        )
        select
            '{{ this.identifier }}' as model_name,
            count(*)                as row_count,
            current_timestamp()     as loaded_at
        from {{ this }}
    {% endset %}

    {{ return(sql) }}
{% endmacro %}
