{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with base as (

    select
        f.driver_key,
        d.driver_full_name,
        d.driver_display_name,
        d.driver_code,
        s.season_year,
        s.season_era,

        f.race_grid_position,
        f.race_final_position_order,
        f.race_points,
        f.status_outcome

    from {{ ref('fct_F1__race_results') }} f
    left join {{ ref('dim_F1__drivers') }} d
        on f.driver_key = d.driver_key
    left join {{ ref('dim_F1__seasons') }} s
        on f.season_key = s.season_key
),

per_race as (

    select
        *,
        -- Position delta between grid and final result (positive = positions gained)
        case
            when race_grid_position is not null
             and race_final_position_order is not null
            then race_grid_position - race_final_position_order
        end as positions_gained,

        -- Did the driver finish the race? (including "classified" if applicable)
        case 
            when status_outcome in ('FINISHED','CLASSIFIED') 
            then 1 else 0 
        end as finished_flag,

        -- Did the driver record a DNF?
        case 
            when status_outcome = 'DNF' 
            then 1 else 0 
        end as dnf_flag,

        -- Did the driver score points in this race?
        case 
            when race_points > 0 
            then 1 else 0 
        end as points_finish_flag,

        -- "Salvaged race": started P11 or worse and still scored points
        case 
            when race_grid_position > 10 
             and race_points > 0 
            then 1 else 0 
        end as salvaged_race_flag,

        -- "Thrown away race": started in top 5 and finished with 0 points
        case 
            when race_grid_position <= 5 
             and race_points = 0 
            then 1 else 0 
        end as thrown_away_race_flag

    from base
),

aggregated as (

    select
        driver_key,
        max(driver_full_name)                   as driver_full_name,
        max(driver_display_name)                as driver_display_name,
        max(coalesce(driver_code, 'NO DATA'))   as driver_code,
        season_year,
        season_era,

        -- Number of race entries for this driver in the season
        count(*)                              as races_count,

        -- Average positions gained per race (racecraft / overtaking indicator)
        avg(positions_gained)                 as avg_positions_gained,

        -- Share of races finished (reliability / consistency indicator)
        sum(finished_flag) * 1.0 / count(*)   as finish_ratio,

        -- Share of races that ended in DNF
        sum(dnf_flag) * 1.0 / count(*)        as dnf_ratio,

        -- Average points scored per race
        avg(race_points)                      as avg_points_per_race,

        -- Share of races in which the driver scored points
        sum(points_finish_flag) * 1.0 / count(*) as points_finish_ratio,

        -- Among races starting P11 or worse, share of races where the driver still scored points
        -- (ability to "salvage" difficult Sundays). If the driver never started >10, we set 0.0.
        case 
            when sum(case when race_grid_position > 10 then 1 else 0 end) = 0
                then 0.0
            else
                sum(salvaged_race_flag) * 1.0
                / sum(case when race_grid_position > 10 then 1 else 0 end)
        end                                     as salvage_ratio,

        -- Among races starting in top 5, share of races where the driver finished with 0 points
        -- (tendency to "throw away" good starting opportunities). If the driver never started in top 5, we set 0.0.
        case 
            when sum(case when race_grid_position <= 5 then 1 else 0 end) = 0
                then 0.0
            else
                sum(thrown_away_race_flag) * 1.0
                / sum(case when race_grid_position <= 5 then 1 else 0 end)
        end                                     as throw_away_ratio

    from per_race
    group by driver_key, season_year, season_era
)

select *
from aggregated
order by driver_full_name, season_year DESC
