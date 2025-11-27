select *
from {{ ref('mart_F1__driver_racecraft_performance') }}
where
    races_count <= 0
    or (finish_ratio          is not null and (finish_ratio          < 0 or finish_ratio          > 1))
    or (dnf_ratio             is not null and (dnf_ratio             < 0 or dnf_ratio             > 1))
    or (points_finish_ratio   is not null and (points_finish_ratio   < 0 or points_finish_ratio   > 1))
    or (salvage_ratio         is not null and (salvage_ratio         < 0 or salvage_ratio         > 1))
    or (throw_away_ratio      is not null and (throw_away_ratio      < 0 or throw_away_ratio      > 1))
    or (avg_points_per_race   is not null and avg_points_per_race   < 0)
