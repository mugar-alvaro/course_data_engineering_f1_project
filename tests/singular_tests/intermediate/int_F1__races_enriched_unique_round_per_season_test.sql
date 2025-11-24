select
    season_year,
    race_round,
    count(*) as row_count
from {{ ref('int_F1__races_enriched') }}
group by
    season_year,
    race_round
having count(*) > 1