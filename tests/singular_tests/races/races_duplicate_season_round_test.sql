select
    race_year,
    race_round,
    count(*) as row_count
from {{ ref('stg_F1__races') }}
group by 1, 2
having count(*) > 1
