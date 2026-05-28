{% macro unpivot_measurements_from_source(source_name, source_cfg, selected_rows_cte='selected_timestamps') -%}
  {%- set rel = ref(source_cfg["relation"]) -%}
  {%- set raw_id_col = source_cfg.get("raw_id_col", "_airbyte_raw_id") -%}
  {%- set vars_map = source_cfg.get("variables", {}) -%}

  select
    s.source_row_id,
    s.extracted_at,
    s.station_code,
    s.data_source_name,
    s.measured_at_parsed,
    s.cursor_id,
    s.is_measured_at_valid,
    s.measured_at_silver,
    s.is_time_imputed,
    s.time_impute_method,
    v.variable_code,
    v.value_raw,
    v.value_parsed
  from {{ rel }} m
  join {{ selected_rows_cte }} s
    on s.data_source_name = '{{ source_name }}'
   and s.source_row_id = m.{{ raw_id_col }}::text
  cross join lateral (
    values
    {%- for variable_code, col_name in vars_map.items() %}
      (
        '{{ variable_code }}',
        coalesce(m.{{ col_name }}::text, ''),
        m.{{ col_name }}::double precision
      ){% if not loop.last %},{% endif %}
    {%- endfor %}
  ) as v(variable_code, value_raw, value_parsed)
{%- endmacro %}

{% macro measurement_timestamps_from_source(source_name, source_cfg) -%}
  {%- set rel = ref(source_cfg["relation"]) -%}
  {%- set raw_id_col = source_cfg.get("raw_id_col", "_airbyte_raw_id") -%}
  {%- set extracted_col = source_cfg.get("extracted_at_col", "extracted_at") -%}
  {%- set station_col = source_cfg.get("station_code_col", "station_code") -%}
  {%- set measured_col = source_cfg.get("measured_at_col", "measured_at") -%}
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
    {{ is_valid_col }}::boolean as is_measured_at_valid
    {%- else %}
    null::boolean as is_measured_at_valid
    {%- endif %}
  from {{ rel }}
  {% if is_incremental() or measurement_has_ingest_batch_scope() %}
  where {{ measurement_source_incremental_predicate(source_name, extracted_col) }}
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
  {% if is_incremental() or measurement_has_ingest_batch_scope() %}
  where {{ measurement_source_incremental_predicate(source_name, extracted_col) }}
  {% endif %}
{%- endmacro %}

{% macro measurement_stream_candidates_from_source(source_name, source_cfg) -%}
  {%- set vars_map = source_cfg.get("variables", {}) -%}

  select
    max(t.extracted_at) as extracted_at,
    t.station_code,
    v.variable_code,
    '{{ source_name }}' as data_source_name
  from {{ ref('int_measurement_timestamps_silver') }} t
  cross join lateral (
    values
    {%- for variable_code, _col_name in vars_map.items() %}
      ('{{ variable_code }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
  ) as v(variable_code)
  where t.data_source_name = '{{ source_name }}'
  {% if measurement_has_ingest_batch_scope() %}
    and {{ measurement_ingest_extracted_at_predicate('t.extracted_at') }}
  {% elif is_incremental() %}
    and t.extracted_at >= (
      select coalesce(max(existing.extracted_at), '1970-01-01'::timestamptz)
      from {{ this }} existing
      where existing.data_source_name = '{{ source_name }}'
    )
  {% endif %}
  group by t.station_code, v.variable_code
{%- endmacro %}
