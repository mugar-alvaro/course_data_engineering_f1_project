with mart as (
    select
        circuit_key,
        circuit_name,
        circuit_display_name,
        circuit_country,
        circuit_city,
        altitude_meters,
        altitude_category,
        is_high_altitude_circuit,
        hemisphere
    from {{ ref('mart_F1__circuit_performance') }}
),
dim_circuits as (
    select
        circuit_key,
        circuit_name             as dim_circuit_name,
        circuit_display_name     as dim_circuit_display_name,
        country                  as dim_circuit_country,
        city                     as dim_circuit_city,
        altitude_meters          as dim_altitude_meters,
        altitude_category        as dim_altitude_category,
        is_high_altitude_circuit as dim_is_high_altitude_circuit,
        hemisphere               as dim_hemisphere
    from {{ ref('dim_F1__circuits') }}
),
invalid as (
    select
        m.*,
        d.*
    from mart m
    left join dim_circuits d using (circuit_key)
    where
        d.circuit_key is null
        or m.circuit_name          <> d.dim_circuit_name
        or m.circuit_display_name  <> d.dim_circuit_display_name
        or m.circuit_country       <> d.dim_circuit_country
        or m.circuit_city          <> d.dim_circuit_city
        or m.altitude_category     <> d.dim_altitude_category
        or m.is_high_altitude_circuit <> d.dim_is_high_altitude_circuit
        or m.hemisphere            <> d.dim_hemisphere
        or (
            m.altitude_meters is not null
            and d.dim_altitude_meters is not null
            and m.altitude_meters <> d.dim_altitude_meters
        )
)

select *
from invalid
