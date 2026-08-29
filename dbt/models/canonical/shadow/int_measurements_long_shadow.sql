{{ config(materialized='ephemeral') }}

{{ validate_measurement_shadow_scope() }}

{%- set sources_cfg = var('measurements_sources') -%}
{%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}

with selected_timestamps as (
  select
    t.source_row_id,
    t.extracted_at,
    t.station_code,
    t.data_source_name,
    t.measured_at_parsed,
    t.cursor_id,
    t.is_measured_at_valid,
    t.measured_at_silver,
    t.is_time_imputed,
    t.time_impute_method
  from {{ ref('int_measurement_timestamps_silver') }} t
  where {{ measurement_shadow_row_predicate(
    't.data_source_name',
    't.extracted_at',
    't.measured_at_silver'
  ) }}
),

raw_long as (
  {%- for source_name in selected_source_names %}
  {%- set cfg = sources_cfg[source_name] %}

    {{ unpivot_measurements_from_source(source_name, cfg, 'selected_timestamps') }}

    {%- if not loop.last %}
    union all
    {%- endif %}
  {%- endfor %}
)

select * from raw_long
