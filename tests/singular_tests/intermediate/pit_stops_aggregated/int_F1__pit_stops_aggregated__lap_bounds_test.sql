{{ config(
    severity = 'error',
    tags = ['f1', 'pit_stops', 'consistency']
) }}

with bad_rows as (
    select
        race_id,
        driver_id,
        pit_stop_count,
        first_pit_lap_number,
        last_pit_lap_number
    from {{ ref('int_F1__pit_stops_aggregated') }}
    where
        (first_pit_lap_number is not null
         and last_pit_lap_number  is not null
         and first_pit_lap_number > last_pit_lap_number)
        or
        (first_pit_lap_number is not null and first_pit_lap_number <= 0)
        or
        (last_pit_lap_number  is not null and last_pit_lap_number  <= 0)
)

select *
from bad_rows
