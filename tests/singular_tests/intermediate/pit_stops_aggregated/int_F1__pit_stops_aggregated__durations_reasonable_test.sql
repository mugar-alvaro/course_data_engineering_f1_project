{{ config(
    severity = 'warn',
    tags = ['f1', 'pit_stops', 'range_check']
) }}

with suspicious_rows as (
    select
        race_id,
        driver_id,
        pit_stop_count,
        best_pit_stop_duration_milliseconds,
        worst_pit_stop_duration_milliseconds,
        avg_pit_stop_duration_milliseconds,
        total_pit_stop_duration_milliseconds
    from {{ ref('int_F1__pit_stops_aggregated') }}
    where
        (best_pit_stop_duration_milliseconds is not null
         and best_pit_stop_duration_milliseconds <= 0)
        or
        (worst_pit_stop_duration_milliseconds is not null
         and worst_pit_stop_duration_milliseconds <= 0)
        or
        (avg_pit_stop_duration_milliseconds is not null
         and avg_pit_stop_duration_milliseconds <= 0)
        or
        (total_pit_stop_duration_milliseconds is not null
         and total_pit_stop_duration_milliseconds <= 0)
        or
        (best_pit_stop_duration_milliseconds is not null
         and best_pit_stop_duration_milliseconds > 600000)
        or
        (worst_pit_stop_duration_milliseconds is not null
         and worst_pit_stop_duration_milliseconds > 600000)
        or
        (avg_pit_stop_duration_milliseconds is not null
         and avg_pit_stop_duration_milliseconds > 600000)
)

select *
from suspicious_rows
