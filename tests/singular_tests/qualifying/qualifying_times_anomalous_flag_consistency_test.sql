{{ config(severity='warn') }}

with base as (
    select
        q1_time_milliseconds,
        is_anomalous_q1,
        q2_time_milliseconds,
        is_anomalous_q2,
        q3_time_milliseconds,
        is_anomalous_q3,
    from {{ ref('stg_F1__qualifying') }}
)

select *
from base
where 
    (q1_time_milliseconds > 600000 and is_anomalous_q1 = false)
    or
    (q1_time_milliseconds <= 600000 and is_anomalous_q1 = true)
    or
    (q2_time_milliseconds > 600000 and is_anomalous_q2 = false)
    or
    (q2_time_milliseconds <= 600000 and is_anomalous_q2 = true)
    or
    (q3_time_milliseconds > 600000 and is_anomalous_q3 = false)
    or
    (q3_time_milliseconds <= 600000 and is_anomalous_q3 = true)