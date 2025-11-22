select distinct
    r.race_year
from {{ ref('stg_F1__races') }}   r
left join {{ ref('stg_F1__seasons') }} s
    on r.race_year = s.season_year
where s.season_year is null
