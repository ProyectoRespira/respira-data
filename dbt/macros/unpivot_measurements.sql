{% macro unpivot_measurements_from_source(source_name, source_cfg) -%}
  {%- set rel = ref(source_cfg["relation"]) -%}
  {%- set station_col = source_cfg.get("station_code_col", "station_code") -%}
  {%- set measured_col = source_cfg.get("measured_at_col", "measured_at") -%}
  {%- set raw_col = source_cfg.get("raw_payload_col", "raw_payload") -%}
  {%- set vars_map = source_cfg.get("variables", {}) -%}

  {%- set cursor_col = source_cfg.get("cursor_id_col") -%}
  {%- set is_valid_col = source_cfg.get("is_measured_at_valid_col") -%}

  {%- for variable_code, col_name in vars_map.items() %}
    select
      {{ station_col }} as station_code,
      '{{ source_name }}' as data_source_name,

      {{ measured_col }} as measured_at_parsed,

      {%- if cursor_col %}
      {{ cursor_col }}::bigint as cursor_id,
      {%- else %}
      null::bigint as cursor_id,
      {%- endif %}

      {%- if is_valid_col %}
      {{ is_valid_col }}::boolean as is_measured_at_valid,
      {%- else %}
      null::boolean as is_measured_at_valid,
      {%- endif %}

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
