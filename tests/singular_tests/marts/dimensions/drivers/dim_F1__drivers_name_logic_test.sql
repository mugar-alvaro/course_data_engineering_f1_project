with invalid as (
    select *
    from {{ ref('dim_F1__drivers') }}
    where
        driver_full_name <> driver_forename || ' ' || driver_surname
        or driver_display_name <> substr(driver_forename, 1, 1) || '. ' || driver_surname
)

select *
from invalid
