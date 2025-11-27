with dim as (
    select status_id, status_description
    from {{ ref('dim_F1__status') }}
),
stg as (
    select status_id, status_description
    from {{ ref('stg_F1__status') }}
),

invalid as (
    select d.*
    from dim d
    left join stg s using (status_id, status_description)
    where s.status_id is null
)

select *
from invalid
