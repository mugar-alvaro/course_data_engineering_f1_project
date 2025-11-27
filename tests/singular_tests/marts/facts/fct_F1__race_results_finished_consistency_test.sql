select *
from {{ ref('fct_F1__race_results') }}
where
    status_outcome = 'FINISHED'
    and (
        race_laps_completed is null
        or race_laps_completed <= 0
        or race_final_position_order is null
        or race_final_position_order <= 0
        or race_points is null
        or race_points < 0
    )
