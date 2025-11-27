with invalid as (
    select *
    from {{ ref('dim_F1__races') }}
    where
        case
            when race_time_of_day is not null then
                to_timestamp_ntz(race_date || ' ' || race_time_of_day)
            else
                to_timestamp_ntz(race_date)
        end <> race_datetime_utc
        or (
            race_date is not null
            and race_datetime_utc is null
        )
)

select *
from invalid
