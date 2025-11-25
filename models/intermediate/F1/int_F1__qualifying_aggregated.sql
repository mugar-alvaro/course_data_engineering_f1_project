{{ config(
    materialized      = 'incremental',
    unique_key        = ['race_surrogate_key', 'driver_surrogate_key'],
    on_schema_change  = 'append_new_columns',
    post_hook         = "{{ f1_log_model_run() }}"
) }}

with base as (
    select
        qualify_surrogate_key,
        qualify_id,
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        constructor_surrogate_key,
        constructor_id,
        car_number,
        qualifying_position,
        q1_time_milliseconds,
        is_anomalous_q1,
        q2_time_milliseconds,
        is_anomalous_q2,
        q3_time_milliseconds,
        is_anomalous_q3,
        ingestion_timestamp
    from {{ ref('stg_F1__qualifying') }}
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

best_time as (
    select
        *,
        least(
            nullif(q1_time_milliseconds, 0),
            nullif(q2_time_milliseconds, 0),
            nullif(q3_time_milliseconds, 0)
        ) as best_qualifying_time_milliseconds
    from base
),

best_session as (
    select
        *,
        case
            when q3_time_milliseconds is not null
                 and q3_time_milliseconds = best_qualifying_time_milliseconds
                then 'Q3'
            when q2_time_milliseconds is not null
                 and q2_time_milliseconds = best_qualifying_time_milliseconds
                then 'Q2'
            when q1_time_milliseconds is not null
                 and q1_time_milliseconds = best_qualifying_time_milliseconds
                then 'Q1'
            else 'NONE'
        end as best_qualifying_session
    from best_time
),

session_flags as (
    select
        *,
        (case when q1_time_milliseconds is not null then 1 else 0 end +
         case when q2_time_milliseconds is not null then 1 else 0 end +
         case when q3_time_milliseconds is not null then 1 else 0 end)
            as qualifying_sessions_entered,

        case when q2_time_milliseconds is not null then true else false end
            as qualified_for_q2,

        case when q3_time_milliseconds is not null then true else false end
            as qualified_for_q3

    from best_session
)

select
    {{ surrogate_key(['race_surrogate_key', 'driver_surrogate_key']) }} as race_driver_surrogate_key,
    *
from session_flags
