with invalid as (
    select *
    from {{ ref('dim_F1__seasons') }}
    where
        season_name <> 'Season ' || season_year
        or season_decade <> floor(season_year / 10) * 10
)

select *
from invalid
