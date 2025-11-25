{{ config(severity='warn') }}

select
    race_id,
    driver_id,
    laps_from_lap_times,
    laps_from_results,
    lap_match_category,
    lap_data_mismatch_flag
from {{ ref('int_F1__lap_times_aggregated') }}
where lap_match_category in ('small_mismatch', 'large_mismatch')
   or lap_data_mismatch_flag = true
