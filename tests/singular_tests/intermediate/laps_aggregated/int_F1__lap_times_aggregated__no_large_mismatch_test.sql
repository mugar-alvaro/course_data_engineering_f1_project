{{ config(
    severity = 'warn',
    tags = ['f1', 'laps', 'quality']
) }}

with large_mismatches as (
    select
        race_id,
        driver_id,
        laps_from_lap_times,
        laps_from_results,
        lap_match_category
    from {{ ref('int_F1__lap_times_aggregated') }}
    where lap_match_category = 'large_mismatch'
)

select *
from large_mismatches
