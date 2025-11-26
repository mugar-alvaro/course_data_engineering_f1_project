{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with base as (

    select
        f.race_key,
        f.season_key,

        -- Dim Races
        r.race_id,
        r.race_name,
        r.race_display_name,
        r.race_date,
        r.round_number,

        -- Dim Seasons
        se.season_year,
        se.season_era,
        se.season_decade,

        -- Métricas de calidad a nivel piloto-carrera
        f.duplicate_race_result_count,
        f.is_inconsistent_fastest_lap,
        f.lap_match_category,
        f.lap_data_mismatch_flag,
        f.laps_from_lap_times,
        f.race_laps_completed

    from {{ ref('fct_F1__race_results') }} f
    left join {{ ref('dim_F1__races') }} r
        on f.race_key = r.race_key
    left join {{ ref('dim_F1__seasons') }} se
        on f.season_key = se.season_key
),

aggregated as (

    select
        -- Clave del mart
        race_key,

        -- Contexto de carrera
        max(race_id)             as race_id,
        max(race_name)           as race_name,
        max(race_display_name)   as race_display_name,
        max(race_date)           as race_date,
        max(round_number)        as round_number,

        -- Contexto de temporada
        max(season_key)          as season_key,
        max(season_year)         as season_year,
        max(season_era)          as season_era,
        max(season_decade)       as season_decade,

        -- Volumen de filas (piloto-carrera) en la fact
        count(*)                 as driver_result_count,

        -- DUPLICADOS EN RESULTS
        sum(case
                when duplicate_race_result_count > 1 then 1
                else 0
            end)                 as duplicated_result_rows_count,

        -- FASTEST LAP INCONSISTENTE
        sum(case
                when is_inconsistent_fastest_lap then 1
                else 0
            end)                 as inconsistent_fastest_lap_count,

        -- VUELTAS / LAP TIMES
        sum(case
                when lap_match_category = 'match' then 1
                else 0
            end)                 as lap_match_count,

        sum(case
                when lap_match_category = 'small_mismatch' then 1
                else 0
            end)                 as lap_small_mismatch_count,

        sum(case
                when lap_match_category = 'large_mismatch' then 1
                else 0
            end)                 as lap_large_mismatch_count,

        sum(case
                when lap_match_category = 'only_results' then 1
                else 0
            end)                 as lap_only_results_count,

        sum(case
                when lap_match_category = 'only_lap_times' then 1
                else 0
            end)                 as lap_only_lap_times_count,

        sum(case
                when lap_match_category = 'no_data' then 1
                else 0
            end)                 as lap_no_data_count,

        -- FLAG genérico de mismatch
        sum(case
                when lap_data_mismatch_flag then 1
                else 0
            end)                 as lap_data_mismatch_count,

        -- COBERTURA DE LAP TIMES
        sum(case
                when laps_from_lap_times is not null then 1
                else 0
            end)                 as drivers_with_lap_times_count,

        sum(case
                when laps_from_lap_times is null
                 and race_laps_completed is not null
                then 1
                else 0
            end)                 as drivers_missing_lap_times_count,

        -- Ratios (todos normalizados por driver_result_count)
        (sum(case when duplicate_race_result_count > 1 then 1 else 0 end)
            * 1.0 / count(*)
        )                        as duplicated_result_ratio,

        (sum(case when is_inconsistent_fastest_lap then 1 else 0 end)
            * 1.0 / count(*)
        )                        as inconsistent_fastest_lap_ratio,

        (sum(case when lap_data_mismatch_flag then 1 else 0 end)
            * 1.0 / count(*)
        )                        as lap_data_mismatch_ratio,

        (sum(case
                when laps_from_lap_times is not null then 1 else 0 end
            ) * 1.0 / count(*)
        )                        as lap_times_coverage_ratio,

        (sum(case
                when laps_from_lap_times is null
                 and race_laps_completed is not null
                then 1 else 0 end
            ) * 1.0 / count(*)
        )                        as missing_lap_times_ratio

    from base
    group by
        race_key
)

select *
from aggregated
