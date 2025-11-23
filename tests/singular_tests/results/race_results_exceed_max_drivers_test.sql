select race_id, count(*)
from {{ ref('stg_F1__results') }}
where race_position is not null
group by race_id
having count(*) > 33
