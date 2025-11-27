{{ config(
    materialized      = 'incremental',
    unique_key        = ['race_surrogate_key', 'driver_surrogate_key'],
    on_schema_change  = 'append_new_columns',
    post_hook         = "{{ f1_log_model_run() }}"
) }}

with watermark as (

    select
        {% if var('f1_use_incremental', true) and is_incremental() %}
            coalesce(max(ingestion_timestamp), '1900-01-01'::timestamp)
        {% else %}
            '1900-01-01'::timestamp
        {% endif %}
        as max_ingestion_timestamp
    {% if var('f1_use_incremental', true) and is_incremental() %}
        from {{ this }}
    {% endif %}

),

laps as (

    select
        lt.race_surrogate_key,
        lt.race_id,
        lt.driver_surrogate_key,
        lt.driver_id,
        lt.lap_number,
        lt.lap_position,
        lt.lap_time_milliseconds,
        lt.is_anomalous_lap,
        -- blindamos ya aquí, por si acaso
        coalesce(lt.ingestion_timestamp, current_timestamp()) as ingestion_timestamp
    from {{ ref('stg_F1__lap_times') }} as lt

    {% if var('f1_use_incremental', true) and is_incremental() %}
    join watermark w
        on 1 = 1
    where lt.ingestion_timestamp > w.max_ingestion_timestamp
    {% endif %}

),

aggregated_laps as (

    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        count(*) as laps_from_lap_times,

        min(case
                when is_anomalous_lap = false
                then lap_time_milliseconds
            end) as best_lap_time_milliseconds,

        avg(case
                when is_anomalous_lap = false
                then lap_time_milliseconds
            end) as avg_lap_time_milliseconds,

        sum(case
                when is_anomalous_lap = false
                then lap_time_milliseconds
            end) as total_lap_time_milliseconds,

        min(lap_position) as best_lap_position,
        max(lap_position) as worst_lap_position,
        avg(lap_position) as avg_lap_position,

        -- si por lo que sea todos los ingestion vienen null (raro), metemos ahora
        max(coalesce(ingestion_timestamp, current_timestamp())) as ingestion_timestamp
    from laps
    group by 1,2,3,4
),

results as (

    select
        r.race_surrogate_key,
        r.race_id,
        r.driver_surrogate_key,
        r.driver_id,
        r.race_laps_completed,
        -- blindamos también aquí, por si hay algun null en la STG de results
        coalesce(r.ingestion_timestamp, current_timestamp()) as ingestion_timestamp
    from {{ ref('stg_F1__results') }} as r

    {% if var('f1_use_incremental', true) and is_incremental() %}
    join watermark w
        on 1 = 1
    where r.ingestion_timestamp > w.max_ingestion_timestamp
    {% endif %}

),

joined as (

    select
        coalesce(a.race_surrogate_key,   r.race_surrogate_key)   as race_surrogate_key,
        coalesce(a.race_id,              r.race_id)              as race_id,
        coalesce(a.driver_surrogate_key, r.driver_surrogate_key) as driver_surrogate_key,
        coalesce(a.driver_id,            r.driver_id)            as driver_id,

        a.laps_from_lap_times,
        r.race_laps_completed                                    as laps_from_results,
        a.best_lap_time_milliseconds,
        a.avg_lap_time_milliseconds,
        a.total_lap_time_milliseconds,
        a.best_lap_position,
        a.worst_lap_position,
        a.avg_lap_position,

        -- AQUÍ lo hacemos a prueba de bombas:
        coalesce(
            greatest(a.ingestion_timestamp, r.ingestion_timestamp),
            a.ingestion_timestamp,
            r.ingestion_timestamp,
            current_timestamp()
        ) as ingestion_timestamp,

        case
            when a.laps_from_lap_times is null
             and r.race_laps_completed is not null then 'only_results'
            when a.laps_from_lap_times is not null
             and r.race_laps_completed is null then 'only_lap_times'
            when a.laps_from_lap_times is null
             and r.race_laps_completed is null then 'no_data'
            when a.laps_from_lap_times = r.race_laps_completed then 'match'
            when abs(a.laps_from_lap_times - r.race_laps_completed) <= 5 then 'small_mismatch'
            else 'large_mismatch'
        end as lap_match_category,

        case
            when a.laps_from_lap_times is not null
             and r.race_laps_completed is not null
             and a.laps_from_lap_times <> r.race_laps_completed
            then true
            else false
        end as lap_data_mismatch_flag
    from aggregated_laps a
    full outer join results r
        on a.race_id   = r.race_id
       and a.driver_id = r.driver_id
)

select
    {{ surrogate_key(['race_surrogate_key', 'driver_surrogate_key']) }} as race_driver_surrogate_key,
    *
from joined
