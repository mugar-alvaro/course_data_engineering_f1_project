select
    race_id,
    driver_id,
    laps_from_lap_times,
    laps_from_results,
    lap_match_category
from {{ ref('int_F1__lap_times_aggregated') }}
where laps_from_lap_times is not null
  and laps_from_results   is not null
  and lap_match_category in ('only_results', 'only_lap_times', 'no_data')
