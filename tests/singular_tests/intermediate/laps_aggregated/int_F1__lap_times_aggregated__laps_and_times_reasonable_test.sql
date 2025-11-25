{{ config(
    severity = 'warn',
    tags = ['f1', 'laps', 'range_check']
) }}

with suspicious_rows as (
    select
        race_id,
        driver_id,
        laps_from_lap_times,
        laps_from_results,
        best_lap_time_milliseconds,
        avg_lap_time_milliseconds,
        total_lap_time_milliseconds
    from {{ ref('int_F1__lap_times_aggregated') }}
    where
        (laps_from_lap_times is not null 
         and (laps_from_lap_times < 0 or laps_from_lap_times > 200))
        or
        (laps_from_results is not null 
         and (laps_from_results < 0 or laps_from_results > 200))
        or
        (
          laps_from_lap_times >= 3
          and laps_from_results    >= 3
          and (
              best_lap_time_milliseconds is not null
              and best_lap_time_milliseconds <= 0
              or
              avg_lap_time_milliseconds is not null
              and avg_lap_time_milliseconds <= 0
              or
              total_lap_time_milliseconds is not null
              and total_lap_time_milliseconds <= 0
          )
        )
)

select *
from suspicious_rows
