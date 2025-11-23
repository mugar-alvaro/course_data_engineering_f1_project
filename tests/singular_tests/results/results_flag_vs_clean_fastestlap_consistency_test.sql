select *
from {{ ref('stg_F1__results') }}
where is_inconsistent_fastest_lap = true
  and race_fastest_lap is not null