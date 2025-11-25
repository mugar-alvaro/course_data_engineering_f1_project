select
    race_id,
    constructor_id,
    count(*) as row_count
from {{ ref('stg_F1__constructor_standings') }}
group by 1, 2
having count(*) > 1
