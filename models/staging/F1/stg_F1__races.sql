{{ config(
    materialized = 'table'
) }}

with source as (
    select
        raceId,
        year,
        round,
        circuitId,
        name,
        date,
        time,
        fp1_date,
        fp1_time,
        fp2_date,
        fp2_time,
        fp3_date,
        fp3_time,
        quali_date,
        quali_time,
        sprint_date,
        sprint_time,
        ingestion_timestamp
    from {{ source('F1', 'races') }}
),

cleaned as (
    select
        {{ surrogate_key(['raceId']) }}       as race_surrogate_key,
        raceId                                as race_id,
        {{ surrogate_key(['circuitId']) }}    as circuit_surrogate_key,
        circuitId                             as circuit_id,
        cast(year as number(4,0))             as race_year,
        cast(round as number(3,0))            as race_round,
        trim(name)                            as race_name,
        cast(date as date)                    as race_date,
        cast(time as time)                    as race_time_of_day,
        cast(fp1_date   as date)              as fp1_session_date,
        cast(fp1_time   as time)              as fp1_session_time,
        cast(fp2_date   as date)              as fp2_session_date,
        cast(fp2_time   as time)              as fp2_session_time,
        cast(fp3_date   as date)              as fp3_session_date,
        cast(fp3_time   as time)              as fp3_session_time,
        cast(quali_date as date)              as quali_session_date,
        cast(quali_time as time)              as quali_session_time,
        cast(sprint_date as date)             as sprint_session_date,
        cast(sprint_time as time)             as sprint_session_time,
        ingestion_timestamp
    from source
)

select * from cleaned;
