{% macro f1_log_model_run() %}
    insert into ALUMNO5_DEV_SILVER_DB.F1.models_run_audit (
        model_name,
        row_count,
        loaded_at
    )
    select
        '{{ this.identifier }}' as model_name,
        count(*)                as row_count,
        current_timestamp()     as loaded_at
    from {{ this }};
{% endmacro %}