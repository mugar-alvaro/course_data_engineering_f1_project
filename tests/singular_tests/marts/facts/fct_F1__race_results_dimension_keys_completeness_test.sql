select *
from {{ ref('fct_F1__race_results') }}
where
    (race_id        is not null and race_key        is null)
    or (driver_id       is not null and driver_key      is null)
    or (constructor_id  is not null and constructor_key is null)
    or (race_status_id  is not null and status_key      is null)
    or (race_key is not null and (season_key is null or circuit_key is null))
