{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with base as (

    select
        f.circuit_key,
        f.season_key,

        c.circuit_name,
        c.circuit_display_name,
        c.country            as circuit_country,
        c.city               as circuit_city,
        c.altitude_meters,
        c.altitude_category,
        c.is_high_altitude_circuit,
        c.hemisphere,

        se.season_year,
        se.season_era,
        se.season_decade,

        f.race_points,
        f.race_final_position_order,
        f.race_grid_position,
        f.status_outcome,
        f.pit_stop_count,
        f.best_lap_time_milliseconds

    from {{ ref('fct_F1__race_results') }} f
    left join {{ ref('dim_F1__circuits') }} c
        on f.circuit_key = c.circuit_key
    left join {{ ref('dim_F1__seasons') }} se
        on f.season_key = se.season_key
),

aggregated as (

    select
        -- Keys
        circuit_key,
        season_era,

        -- Circuit attributes
        max(circuit_name)              as circuit_name,
        max(circuit_display_name)      as circuit_display_name,
        max(circuit_country)           as circuit_country,
        max(circuit_city)              as circuit_city,
        max(altitude_meters)           as altitude_meters,
        max(altitude_category)         as altitude_category,
        max(is_high_altitude_circuit)  as is_high_altitude_circuit,
        max(hemisphere)                as hemisphere,
 
        -- Volume metrics
        count(*)                       as driver_race_count,        -- driver-race rows
        count(distinct season_year)    as distinct_seasons_count,
        count(distinct season_decade)  as distinct_decades_count,
 
        -- Position dynamics (overtaking / degradation)
        avg(
            case
                when race_grid_position is not null
                 and race_final_position_order is not null
                then race_grid_position - race_final_position_order
            end
        )                               as avg_positions_gained,     -- >0 = overtaking circuit, <0 = positional loss

        sum(
            case
                when race_grid_position is not null
                 and race_final_position_order is not null
                 and race_grid_position - race_final_position_order > 0
                then 1 else 0
            end
        ) * 1.0 / count(*)             as gain_positions_ratio,     -- % of entries gaining positions

        sum(
            case
                when race_grid_position is not null
                 and race_final_position_order is not null
                 and race_grid_position - race_final_position_order < 0
                then 1 else 0
            end
        ) * 1.0 / count(*)             as lose_positions_ratio,     -- % of entries losing positions

        -- Qualifying importance & points distribution
        sum(case when race_points > 0 then 1 else 0 end) * 1.0 / count(*) 
                                        as points_finish_ratio,     -- % of drivers scoring points

        avg(
            case
                when race_points > 0 then race_grid_position
            end
        )                               as avg_grid_position_points_finishers,
        -- Typical starting position of points finishers: closer to 1 means qualifying is crucial

        -- Pole effectiveness (how often pole converts to win)
        sum(
            case 
                when race_grid_position = 1 
                 and race_final_position_order = 1 
                then 1 else 0 
            end
        ) * 1.0
        / nullif(
            sum(case when race_grid_position = 1 then 1 else 0 end),
            0
        )                               as pole_to_win_ratio,

        -- Reliability profile (mechanical issues / incidents)
        sum(case when status_outcome = 'DNF'          then 1 else 0 end) * 1.0 / count(*) 
                                        as dnf_ratio,               -- % of did-not-finish
        sum(case when status_outcome = 'FINISHED'     then 1 else 0 end) * 1.0 / count(*) 
                                        as finished_ratio,          -- % finished
        sum(case when status_outcome = 'DISQUALIFIED' then 1 else 0 end) * 1.0 / count(*) 
                                        as disqualified_ratio,      -- % disqualified

        -- Strategy profile (pit intensity)
        avg(pit_stop_count)             as avg_pit_stops_per_entry,

        -- Pace profile (aggregated clean best-lap data)
        avg(best_lap_time_milliseconds) as avg_best_lap_time_ms

    from base
    group by
        circuit_key,
        season_era
)

select *
from aggregated
order by circuit_name, season_era