{{ config(
    severity = 'error',
    tags = ['f1', 'pit_stops', 'consistency']
) }}

with stg_aggregated as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        count(*) as pit_stop_count_stg
    from {{ ref('stg_F1__pit_stops') }}
    group by 1,2,3,4
),

int_aggregated as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        pit_stop_count as pit_stop_count_int
    from {{ ref('int_F1__pit_stops_aggregated') }}
),

joined as (
    select
        coalesce(s.race_surrogate_key,   i.race_surrogate_key)   as race_surrogate_key,
        coalesce(s.race_id,              i.race_id)              as race_id,
        coalesce(s.driver_surrogate_key, i.driver_surrogate_key) as driver_surrogate_key,
        coalesce(s.driver_id,            i.driver_id)            as driver_id,
        s.pit_stop_count_stg,
        i.pit_stop_count_int
    from stg_aggregated s
    full outer join int_aggregated i
        on  s.race_id   = i.race_id
        and s.driver_id = i.driver_id
)

select *
from joined
where
    (pit_stop_count_stg is null and pit_stop_count_int is not null)
    or
    (pit_stop_count_stg is not null and pit_stop_count_int is null)
    or
    (pit_stop_count_stg is not null
     and pit_stop_count_int is not null
     and pit_stop_count_stg <> pit_stop_count_int)
