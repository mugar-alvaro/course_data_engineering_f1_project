select
    race_id,
    driver_id,
    fastest_lap_number,
    fastest_lap_rank
from {{ ref('int_F1__race_results_enriched') }}
where
      (fastest_lap_number is not null and fastest_lap_rank is null)
   or (fastest_lap_number is null and fastest_lap_rank is not null)