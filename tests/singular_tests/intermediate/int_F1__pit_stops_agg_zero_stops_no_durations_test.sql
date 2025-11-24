select
    race_id,
    driver_id,
    pit_stop_count,
    best_pit_stop_seconds,
    avg_pit_stop_seconds,
    total_pit_stop_seconds
from {{ ref('int_F1__pit_stops_aggregated') }}
where pit_stop_count = 0
  and (
        best_pit_stop_seconds  is not null
     or avg_pit_stop_seconds   is not null
     or total_pit_stop_seconds is not null
  )
