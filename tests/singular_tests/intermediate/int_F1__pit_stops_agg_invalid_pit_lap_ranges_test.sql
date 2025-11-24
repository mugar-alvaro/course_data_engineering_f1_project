select
    race_id,
    driver_id,
    pit_stop_count,
    first_pit_lap,
    last_pit_lap
from {{ ref('int_F1__pit_stops_aggregated') }}
where pit_stop_count > 0
  and (
        first_pit_lap is null
     or last_pit_lap  is null
     or first_pit_lap > last_pit_lap
  );