select
    race_id,
    count(*) as constructors_in_standings
from {{ ref('stg_F1__constructor_standings') }}
group by 1
having count(*) > 24
