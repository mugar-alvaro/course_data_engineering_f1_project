{% macro f1_clean_fastest_lap(fastest_lap_col, laps_col) %}
    case
        when {{ fastest_lap_col }} is null then null
        when {{ fastest_lap_col }} > {{ laps_col }}
            then null
        else {{ fastest_lap_col }}
    end
{% endmacro %}


{% macro f1_flag_inconsistent_fastest_lap(fastest_lap_col, laps_col) %}
    case
        when {{ fastest_lap_col }} is null then false
        when {{ fastest_lap_col }} > {{ laps_col }}
            then true
        else false
    end
{% endmacro %}
