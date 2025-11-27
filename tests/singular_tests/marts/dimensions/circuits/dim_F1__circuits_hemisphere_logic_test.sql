with invalid as (
    select *
    from {{ ref('dim_F1__circuits') }}
    where
        (latitude >= 0 and hemisphere <> 'NORTH')
        or (latitude < 0 and hemisphere <> 'SOUTH')
)

select *
from invalid
