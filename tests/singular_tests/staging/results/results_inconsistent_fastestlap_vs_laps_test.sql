select *
from {{ ref('stg_F1__results') }}
where race_fastest_lap > race_laps_completed
