{{ config(
    severity = 'error',
    tags = ['f1', 'qualifying', 'consistency']
) }}

with bad_rows as (
    select
        race_id,
        driver_id,
        qualifying_sessions_entered,
        q1_time_milliseconds,
        q2_time_milliseconds,
        q3_time_milliseconds
    from {{ ref('int_F1__qualifying_enriched') }}
    where
        qualifying_sessions_entered != (
            case when q1_time_milliseconds is not null then 1 else 0 end +
            case when q2_time_milliseconds is not null then 1 else 0 end +
            case when q3_time_milliseconds is not null then 1 else 0 end
        )
        or qualifying_sessions_entered < 0
        or qualifying_sessions_entered > 3
)

select *
from bad_rows
