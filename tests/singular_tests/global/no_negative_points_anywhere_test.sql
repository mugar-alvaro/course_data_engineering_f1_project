with all_points as (
    select
        'race_result' as source_table,
        race_id,
        driver_id,
        race_points as points
    from {{ ref('stg_F1__results') }}

    union all

    select
        'sprint_result' as source_table,
        race_id,
        driver_id,
        sprint_points as points
    from {{ ref('stg_F1__sprint_results') }}

    union all

    select
        'driver_standing' as source_table,
        race_id,
        driver_id,
        driver_points as points
    from {{ ref('stg_F1__driver_standings') }}

    union all

    select
        'constructor_standing' as source_table,
        race_id,
        null as driver_id,
        constructor_points as points
    from {{ ref('stg_F1__constructor_standings') }}
)

select *
from all_points
where points < 0
