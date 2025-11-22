select *
from {{ ref('stg_F1__results') }}
where race_fastest_lap is not null
  and race_fastest_lap_time_milliseconds is null
