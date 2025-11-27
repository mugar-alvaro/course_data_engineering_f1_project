select *
from {{ ref('mart_F1__circuit_performance') }}
where
    driver_race_count         < 0
    or distinct_seasons_count < 0
    or distinct_decades_count < 0
    or (gain_positions_ratio       is not null and (gain_positions_ratio       < 0 or gain_positions_ratio       > 1))
    or (lose_positions_ratio       is not null and (lose_positions_ratio       < 0 or lose_positions_ratio       > 1))
    or (points_finish_ratio        is not null and (points_finish_ratio        < 0 or points_finish_ratio        > 1))
    or (pole_to_win_ratio          is not null and (pole_to_win_ratio          < 0 or pole_to_win_ratio          > 1))
    or (dnf_ratio                  is not null and (dnf_ratio                  < 0 or dnf_ratio                  > 1))
    or (finished_ratio             is not null and (finished_ratio             < 0 or finished_ratio             > 1))
    or (disqualified_ratio         is not null and (disqualified_ratio         < 0 or disqualified_ratio         > 1))
    or (
        dnf_ratio        is not null
        and finished_ratio is not null
        and disqualified_ratio is not null
        and (dnf_ratio + finished_ratio + disqualified_ratio) > 1.000001
    )
    or (avg_pit_stops_per_entry        is not null and avg_pit_stops_per_entry        < 0)
    or (avg_best_lap_time_ms           is not null and avg_best_lap_time_ms           < 0)
