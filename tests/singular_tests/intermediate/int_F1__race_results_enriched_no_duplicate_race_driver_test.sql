select
    race_id,
    driver_id,
    count(*) as row_count
from {{ ref('int_F1__race_results_enriched') }}
group by
    race_id,
    driver_id
having count(*) > 1