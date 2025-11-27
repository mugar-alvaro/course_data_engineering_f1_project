with invalid as (
    select *
    from {{ ref('dim_F1__races') }}
    where
        (
            race_date is not null
            and (
                race_weekday_name   <> dayname(race_date)
                or race_month_number <> month(race_date)
                or race_month_name   <> monthname(race_date)
            )
        )
        or
        (
            race_date is null
            and (
                race_weekday_name is not null
                or race_month_number is not null
                or race_month_name is not null
            )
        )
)

select *
from invalid
