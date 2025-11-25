{{ config(
    severity = 'error',
    tags = ['f1', 'qualifying', 'accepted_values']
) }}

with invalid_values as (
    select distinct best_qualifying_session
    from {{ ref('int_F1__qualifying_enriched') }}
    where best_qualifying_session not in ('Q1', 'Q2', 'Q3', 'NONE')
)

select *
from invalid_values;
