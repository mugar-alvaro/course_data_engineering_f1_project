{% macro f1_log_model_run() %}
    /*
      Inserta un registro de auditoría cada vez que se construye
      un modelo que use este post_hook.

      Se espera que exista la tabla:
        ALUMNO5_DEV_BRONZE_DB.F1.models_run_audit
    */
    insert into ALUMNO5_DEV_BRONZE_DB.F1.models_run_audit (
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