{{ config(severity='warn') }}

with laps as (
    select
        race_id,
        driver_id,
        count(*) as lap_count
    from {{ ref('stg_F1__lap_times') }}
    group by 1, 2
),

joined as (
    select
        r.race_id,
        r.driver_id,
        r.race_laps_completed,
        l.lap_count,
        r.race_laps_completed - l.lap_count as lap_diff
    from {{ ref('stg_F1__results') }} r
    left join laps l
        on  r.race_id = l.race_id
        and r.driver_id = l.driver_id
    where r.race_laps_completed is not null
      and l.lap_count is not null
)

select
    race_id,
    driver_id,
    race_laps_completed,
    lap_count,
    lap_diff
from joined
where lap_diff > 5
