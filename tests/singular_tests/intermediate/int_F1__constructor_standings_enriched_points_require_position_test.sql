select
    race_id,
    constructor_id,
    points,
    position
from {{ ref('int_F1__constructor_standings_enriched') }}
where points is not null
  and points > 0
  and position is null