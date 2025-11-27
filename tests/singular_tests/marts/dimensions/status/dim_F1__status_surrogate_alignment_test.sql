with duplicates as (
    select 
        status_id,
        count(distinct status_key) as sk_count
    from {{ ref('dim_F1__status') }}
    group by status_id
    having count(distinct status_key) > 1
)

select *
from duplicates
