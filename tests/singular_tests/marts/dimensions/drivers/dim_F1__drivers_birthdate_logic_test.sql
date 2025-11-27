with invalid as (
    select *
    from {{ ref('dim_F1__drivers') }}
    where
        (
            date_of_birth is not null
            and (
                birth_year  <> year(date_of_birth)
                or birth_decade <> floor(year(date_of_birth) / 10) * 10
                or date_of_birth > current_date()
            )
        )
        or (
            date_of_birth is null
            and (birth_year is not null or birth_decade is not null)
        )
)

select *
from invalid
