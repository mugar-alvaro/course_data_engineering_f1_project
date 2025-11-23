{{ config(
    materialized = 'view'
) }}

with source as (
    select
        driverId,
        driverRef,
        number,
        code,
        forename,
        surname,
        dob,
        nationality,
        ingestion_timestamp
    from {{ source('F1', 'drivers') }}
),

cleaned as (
    select
        {{ surrogate_key(['driverId']) }}     as driver_surrogate_key,
        driverId                              as driver_id,
        trim(driverRef)                       as driver_reference,
        cast(number as number(3,0))           as driver_number,
        trim(code)                            as driver_code,
        trim(forename)                        as driver_forename,
        trim(surname)                         as driver_surname,
        cast(dob as date)                     as date_of_birth,
        trim(nationality)                     as driver_nationality,
        ingestion_timestamp
    from source
)

select * from cleaned
