{{ config(severity='warn') }}

with base as (
    select
        lap_time_milliseconds,
        is_anomalous_lap
    from {{ ref('stg_F1__lap_times') }}
)

select *
from base
where 
    (lap_time_milliseconds > 600000 and is_anomalous_lap = false)
    or
    (lap_time_milliseconds <= 600000 and is_anomalous_lap = true)
