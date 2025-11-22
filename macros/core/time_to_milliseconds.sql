{% macro f1_time_to_ms(col) %}
    case
        when {{ col }} is null then null

        -- Formato con horas: H:MM:SS.mmm
        when regexp_count({{ col }}, ':') = 2 then
              (cast(split_part(trim({{ col }}), ':', 1) as number) * 3600000)
            + (cast(split_part(trim({{ col }}), ':', 2) as number) * 60000)
            + (cast(split_part(split_part(trim({{ col }}), ':', 3), '.', 1) as number) * 1000)
            +  cast(split_part(split_part(trim({{ col }}), ':', 3), '.', 2) as number)

        -- Formato con minutos: M:SS.mmm
        when position(':' in {{ col }}) > 0 then
              (cast(split_part({{ col }}, ':', 1) as number) * 60000)
            + (cast(split_part(split_part({{ col }}, ':', 2), '.', 1) as number) * 1000)
            +  cast(split_part(split_part({{ col }}, ':', 2), '.', 2) as number)
        -- Formato solo segundos: SSS.mmm
        else
              (cast(split_part({{ col }}, '.', 1) as number) * 1000)
            +  cast(split_part({{ col }}, '.', 2) as number)
    end
{% endmacro %}
