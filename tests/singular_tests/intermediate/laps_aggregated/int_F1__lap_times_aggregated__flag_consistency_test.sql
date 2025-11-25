{{ config(
    severity = 'error',
    tags = ['f1', 'laps', 'consistency']
) }}

with bad_rows as (
    select *
    from {{ ref('int_F1__lap_times_aggregated') }}
    where
        (
            lap_data_mismatch_flag = true
            and (
                laps_from_lap_times is null
                or laps_from_results    is null
                or laps_from_lap_times = laps_from_results
            )
        )
        or
        (
            lap_data_mismatch_flag = false
            and laps_from_lap_times is not null
            and laps_from_results    is not null
            and laps_from_lap_times <> laps_from_results
        )
)

select *
from bad_rows
