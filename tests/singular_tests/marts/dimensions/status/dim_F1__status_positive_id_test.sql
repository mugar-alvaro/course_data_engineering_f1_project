select *
from {{ ref('dim_F1__status') }}
where status_id <= 0
