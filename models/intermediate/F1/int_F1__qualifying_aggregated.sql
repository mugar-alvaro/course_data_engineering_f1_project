-- PURPOSE:
-- Intermediate model that consolidates qualifying data at driver-race grain.
-- It keeps all detailed fields from staging and adds:
--   - best_qualifying_time_milliseconds
--   - best_qualifying_session (Q1 / Q2 / Q3 / NONE)
--   - qualifying_sessions_entered (1–3)
--   - session qualification flags (qualified_for_q2, qualified_for_q3)
-- This model prepares clean qualifying metrics for Gold without losing detail.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
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

select *
from session_flags