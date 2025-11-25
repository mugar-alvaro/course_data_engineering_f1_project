{{ config(
    severity = 'error',
    tags = ['f1', 'qualifying', 'best_session']
) }}

with bad_rows as (
    select
        race_id,
        driver_id,
        best_qualifying_session,
        best_qualifying_time_milliseconds,
        q1_time_milliseconds,
        q2_time_milliseconds,
        q3_time_milliseconds
    from {{ ref('int_F1__qualifying_enriched') }}
    where
        (best_qualifying_session = 'Q1' and
         (q1_time_milliseconds is null
          or q1_time_milliseconds <> best_qualifying_time_milliseconds))
        or
        (best_qualifying_session = 'Q2' and
         (q2_time_milliseconds is null
          or q2_time_milliseconds <> best_qualifying_time_milliseconds))
        or
        (best_qualifying_session = 'Q3' and
         (q3_time_milliseconds is null
          or q3_time_milliseconds <> best_qualifying_time_milliseconds))
        or
        (best_qualifying_session = 'NONE' and
         (
             best_qualifying_time_milliseconds is not null
             or q1_time_milliseconds is not null
             or q2_time_milliseconds is not null
             or q3_time_milliseconds is not null
         ))
        or
        (best_qualifying_session <> 'NONE'
         and best_qualifying_time_milliseconds is null)
)

select *
from bad_rows
