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
        case
            when race_grid_position is not null
             and race_final_position_order is not null
            then race_grid_position - race_final_position_order
        end as positions_gained,

        case 
            when status_outcome in ('FINISHED','CLASSIFIED') 
            then 1 else 0 
        end as finished_flag,

        case 
            when status_outcome = 'DNF' 
            then 1 else 0 
        end as dnf_flag,

        case 
            when race_points > 0 
            then 1 else 0 
        end as points_finish_flag,

        case 
            when race_grid_position > 10 
             and race_points > 0 
            then 1 else 0 
        end as salvaged_race_flag,

        case 
            when race_grid_position <= 5 
             and race_points = 0 
            then 1 else 0 
        end as thrown_away_race_flag
    from base
),

recomputed as (
    select
        driver_key,
        max(driver_full_name)                     as driver_full_name,
        max(driver_display_name)                  as driver_display_name,
        max(coalesce(driver_code, 'NO DATA'))     as driver_code,
        season_year,
        season_era,

        count(*)                                  as races_count,
        avg(positions_gained)                     as avg_positions_gained,
        sum(finished_flag) * 1.0 / count(*)       as finish_ratio,
        sum(dnf_flag) * 1.0 / count(*)            as dnf_ratio,
        avg(race_points)                          as avg_points_per_race,
        sum(points_finish_flag) * 1.0 / count(*)  as points_finish_ratio,

        case 
            when sum(case when race_grid_position > 10 then 1 else 0 end) = 0
                then 0.0
            else
                sum(salvaged_race_flag) * 1.0
                / sum(case when race_grid_position > 10 then 1 else 0 end)
        end                                       as salvage_ratio,

        case 
            when sum(case when race_grid_position <= 5 then 1 else 0 end) = 0
                then 0.0
            else
                sum(thrown_away_race_flag) * 1.0
                / sum(case when race_grid_position <= 5 then 1 else 0 end)
        end                                       as throw_away_ratio

    from per_race
    group by driver_key, season_year, season_era
),

mart as (
    select *
    from {{ ref('mart_F1__driver_racecraft_performance') }}
),

joined as (
    select
        m.*,
        r.races_count              as races_count_calc,
        r.avg_positions_gained     as avg_positions_gained_calc,
        r.finish_ratio             as finish_ratio_calc,
        r.dnf_ratio                as dnf_ratio_calc,
        r.avg_points_per_race      as avg_points_per_race_calc,
        r.points_finish_ratio      as points_finish_ratio_calc,
        r.salvage_ratio            as salvage_ratio_calc,
        r.throw_away_ratio         as throw_away_ratio_calc
    from mart m
    left join recomputed r
      on m.driver_key  = r.driver_key
     and m.season_year = r.season_year
     and m.season_era  = r.season_era
),

invalid as (
    select *
    from joined
    where
        races_count_calc is null
        or races_count              <> races_count_calc
        or avg_positions_gained     <> avg_positions_gained_calc
        or finish_ratio             <> finish_ratio_calc
        or dnf_ratio                <> dnf_ratio_calc
        or avg_points_per_race      <> avg_points_per_race_calc
        or points_finish_ratio      <> points_finish_ratio_calc
        or salvage_ratio            <> salvage_ratio_calc
        or throw_away_ratio         <> throw_away_ratio_calc
)

select *
from invalid
