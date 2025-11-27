with invalid as (
    select *
    from {{ ref('dim_F1__races') }}
    where race_display_name <> season_year || ' - ' || race_name
)

select *
from invalid
