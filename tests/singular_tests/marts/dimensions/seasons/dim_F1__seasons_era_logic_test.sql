with invalid as (
    select *
    from {{ ref('dim_F1__seasons') }}
    where
        case
            when season_year < 1990 then 'CLASSIC'
            when season_year between 1990 and 2013 then 'MODERN'
            else 'HYBRID_ERA'
        end <> season_era
)

select *
from invalid
