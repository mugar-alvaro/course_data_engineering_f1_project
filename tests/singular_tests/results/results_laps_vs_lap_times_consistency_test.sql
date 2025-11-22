
with laps as (
    select
        race_id,
        driver_id,
        count(*) as lap_count
    from {{ ref('stg_F1__lap_times') }}
    group by 1, 2
)

select
    r.race_id,
    r.driver_id,
    r.race_laps_completed,
    l.lap_count
from {{ ref('stg_F1__results') }} r
left join laps l
    on  r.race_id = l.race_id
    and r.driver_id = l.driver_id
where r.race_laps_completed is not null
  and (
        l.lap_count is null
        or r.race_laps_completed > l.lap_count
      )
