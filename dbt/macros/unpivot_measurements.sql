{% macro unpivot_measurements_from_source(source_name, source_cfg) -%}
  {%- set rel = ref(source_cfg["relation"]) -%}
  {%- set station_col = source_cfg.get("station_code_col", "station_code") -%}
  {%- set measured_col = source_cfg.get("measured_at_col", "measured_at") -%}
  {%- set raw_col = source_cfg.get("raw_payload_col", "raw_payload") -%}
  {%- set vars_map = source_cfg.get("variables", {}) -%}

  {%- for variable_code, col_name in vars_map.items() %}
    select
      {{ station_col }} as station_code,
      '{{ source_name }}' as data_source_name,
      {{ measured_col }} as measured_at,
      '{{ variable_code }}' as variable_code,
      {{ col_name }}::text as value_raw,
      {{ col_name }}::double precision as value_parsed,
      {{ raw_col }} as raw_payload
    from {{ rel }}
    where {{ col_name }} is not null

    {%- if not loop.last %}
    union all
    {%- endif %}
  {%- endfor %}
{%- endmacro %}
