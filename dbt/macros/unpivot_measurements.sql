{% macro unpivot_measurements_from_source(source_name, source_cfg) -%}
  {%- set rel = ref(source_cfg["relation"]) -%}
  {%- set raw_id_col = source_cfg.get("raw_id_col", "_airbyte_raw_id") -%}
  {%- set extracted_col = source_cfg.get("extracted_at_col", "extracted_at") -%}
  {%- set station_col = source_cfg.get("station_code_col", "station_code") -%}
  {%- set measured_col = source_cfg.get("measured_at_col", "measured_at") -%}
  {%- set raw_col = source_cfg.get("raw_payload_col", "raw_payload") -%}
  {%- set vars_map = source_cfg.get("variables", {}) -%}

  {%- set cursor_col = source_cfg.get("cursor_id_col") -%}
  {%- set is_valid_col = source_cfg.get("is_measured_at_valid_col") -%}

  select
    {{ raw_id_col }}::text as source_row_id,
    {{ extracted_col }} as extracted_at,
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

    v.variable_code,
    v.value_raw,
    v.value_parsed
  from {{ rel }}
  cross join lateral (
    values
    {%- for variable_code, col_name in vars_map.items() %}
      (
        '{{ variable_code }}',
        coalesce({{ col_name }}::text, ''),
        {{ col_name }}::double precision
      ){% if not loop.last %},{% endif %}
    {%- endfor %}
  ) as v(variable_code, value_raw, value_parsed)
  {% if is_incremental() %}
  where {{ extracted_col }} >= (
      select coalesce(max(extracted_at), '1970-01-01'::timestamptz)
      from {{ this }}
    )
  {% endif %}
{%- endmacro %}

{% macro measurement_payloads_from_source(source_name, source_cfg) -%}
  {%- set rel = ref(source_cfg["relation"]) -%}
  {%- set raw_id_col = source_cfg.get("raw_id_col", "_airbyte_raw_id") -%}
  {%- set extracted_col = source_cfg.get("extracted_at_col", "extracted_at") -%}
  {%- set raw_col = source_cfg.get("raw_payload_col", "raw_payload") -%}

  select
    {{ raw_id_col }}::text as source_row_id,
    {{ extracted_col }} as extracted_at,
    '{{ source_name }}' as data_source_name,
    {{ raw_col }} as raw_payload
  from {{ rel }}
  {% if is_incremental() %}
  where {{ extracted_col }} >= (
    select coalesce(max(extracted_at), '1970-01-01'::timestamptz)
    from {{ this }}
  )
  {% endif %}
{%- endmacro %}
