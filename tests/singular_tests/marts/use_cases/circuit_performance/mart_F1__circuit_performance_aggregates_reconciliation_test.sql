with fact as (
    select
        f.circuit_key,
        se.season_era,
        se.season_year,
        se.season_decade
    from {{ ref('fct_F1__race_results') }} f
    left join {{ ref('dim_F1__seasons') }} se
        on f.season_key = se.season_key
),

recomputed as (
    select
        circuit_key,
        season_era,
        count(*)                      as driver_race_count_calc,
        count(distinct season_year)   as distinct_seasons_count_calc,
        count(distinct season_decade) as distinct_decades_count_calc
    from fact
    group by circuit_key, season_era
),

joined as (
    select
        m.circuit_key,
        m.season_era,
        m.driver_race_count,
        m.distinct_seasons_count,
        m.distinct_decades_count,
        r.driver_race_count_calc,
        r.distinct_seasons_count_calc,
        r.distinct_decades_count_calc
    from {{ ref('mart_F1__circuit_performance') }} m
    left join recomputed r
        on m.circuit_key = r.circuit_key
       and m.season_era  = r.season_era
),

invalid as (
    select *
    from joined
    where
        driver_race_count_calc is null
        or driver_race_count        <> driver_race_count_calc
        or distinct_seasons_count   <> distinct_seasons_count_calc
        or distinct_decades_count   <> distinct_decades_count_calc
)

select *
from invalid
