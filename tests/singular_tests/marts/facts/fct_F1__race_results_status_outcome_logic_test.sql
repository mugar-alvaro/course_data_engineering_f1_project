with fact as (
    select
        fr.*,
        s.status_description
    from {{ ref('fct_F1__race_results') }} fr
    left join {{ ref('dim_F1__status') }} s
        on fr.status_key = s.status_key
),

recalculated as (
    select
        fact.*,
        case
            when upper(status_description) = 'FINISHED'
              or status_description like '+%Lap%'
            then 'FINISHED'

            when upper(status_description) in (
                    'DISQUALIFIED',
                    'EXCLUDED',
                    'UNDERWEIGHT'
                )
            then 'DISQUALIFIED'

            when upper(status_description) in (
                    'DID NOT PREQUALIFY',
                    'DID NOT QUALIFY',
                    '107% RULE',
                    'WITHDREW',
                    'ILLNESS',
                    'INJURED',
                    'INJURY',
                    'DRIVER UNWELL',
                    'DRIVER UNWELL.',
                    'EYE INJURY',
                    'EYE PROBLEM',
                    'SAFETY',
                    'SAFETY CONCERNS'
                )
            then 'NOT_STARTED_OR_CLASSIFIED'

            else 'DNF'
        end as expected_status_outcome
    from fact
),

invalid as (
    select *
    from recalculated
    where status_outcome <> expected_status_outcome
)

select *
from invalid
