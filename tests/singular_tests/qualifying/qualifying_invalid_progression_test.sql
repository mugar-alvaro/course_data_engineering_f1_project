select *
from {{ ref('stg_F1__qualifying') }}
where q1_time_milliseconds is null
  and (q2_time_milliseconds is not null or q3_time_milliseconds is not null)
