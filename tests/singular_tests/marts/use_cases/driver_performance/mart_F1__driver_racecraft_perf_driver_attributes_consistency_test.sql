with mart as (
    select
        driver_key,
        driver_full_name,
        driver_display_name,
        driver_code
    from {{ ref('mart_F1__driver_racecraft_performance') }}
),
dim_drivers as (
    select
        driver_key,
        driver_full_name      as dim_driver_full_name,
        driver_display_name   as dim_driver_display_name,
        driver_code           as dim_driver_code
    from {{ ref('dim_F1__drivers') }}
),
invalid as (
    select
        m.*,
        d.*
    from mart m
    left join dim_drivers d using (driver_key)
    where
        d.driver_key is null
        or m.driver_full_name    <> d.dim_driver_full_name
        or m.driver_display_name <> d.dim_driver_display_name
        or m.driver_code <> coalesce(d.dim_driver_code, 'NO DATA')
)

select *
from invalid
