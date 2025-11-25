{{ config(
    severity = 'error',
    tags = ['f1', 'laps', 'accepted_values']
) }}

with invalid_values as (
    select distinct lap_match_category
    from {{ ref('int_F1__lap_times_aggregated') }}
    where lap_match_category not in (
        'only_results',
        'only_lap_times',
        'no_data',
        'match',
        'small_mismatch',
        'large_mismatch'
    )
)

select *
from invalid_values
