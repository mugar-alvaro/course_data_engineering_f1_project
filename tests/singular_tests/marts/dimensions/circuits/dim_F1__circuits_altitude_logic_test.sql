with invalid as (
    select *
    from {{ ref('dim_F1__circuits') }}
    where
        (altitude_meters is null
         and altitude_category <> 'UNKNOWN')
        or (altitude_meters is not null
            and altitude_meters < 200
            and altitude_category <> 'LOW')
        or (altitude_meters between 200 and 800
            and altitude_category <> 'MEDIUM')
        or (altitude_meters > 800
            and altitude_category <> 'HIGH')
        or (altitude_meters >= 1000
            and (is_high_altitude_circuit is null
                 or is_high_altitude_circuit = false))

        or (altitude_meters < 1000
            and altitude_meters is not null
            and is_high_altitude_circuit = true)
)

select *
from invalid

