with counts as (
    select
        race_id,
        driver_id,
        count(*) as row_count
    from {{ ref('fct_F1__race_results') }}
    group by 1, 2
)

select *
from counts
where row_count > 1
