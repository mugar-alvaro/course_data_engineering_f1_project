{{ config(
    severity = 'warn',
    tags = ['f1', 'qualifying', 'flags']
) }}

with bad_rows as (
    select
        race_id,
        driver_id,
        q1_time_milliseconds,
        q2_time_milliseconds,
        q3_time_milliseconds,
        qualified_for_q2,
        qualified_for_q3
    from {{ ref('int_F1__qualifying_enriched') }}
    where
        (qualified_for_q2 = true  and q2_time_milliseconds is null)
        or
        (qualified_for_q2 = false and q2_time_milliseconds is not null)

        or
        (qualified_for_q3 = true  and q3_time_milliseconds is null)
        or
        (qualified_for_q3 = false and q3_time_milliseconds is not null)

        or
        (q3_time_milliseconds is not null
         and (q2_time_milliseconds is null or qualified_for_q2 = false))
)

select *
from bad_rows
