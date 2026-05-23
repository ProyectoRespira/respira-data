{{ config(
  materialized='incremental',
  unique_key=['source_row_id', 'variable_code'],
  incremental_strategy='merge',
  indexes=[
    {'columns': ['source_row_id', 'variable_code'], 'unique': true},
    {'columns': ['extracted_at']}
  ]
) }}

{%- set sources_cfg = var('measurements_sources') -%}

with raw_long as (

  {%- for source_name, cfg in sources_cfg.items() %}

    {{ unpivot_measurements_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

),

timestamps as (

  select
    source_row_id,
    data_source_name,
    measured_at_silver,
    is_time_imputed,
    time_impute_method
  from {{ ref('int_measurement_timestamps_silver') }}

),

long as (

  select
    m.source_row_id,
    m.extracted_at,
    m.station_code,
    m.data_source_name,
    m.measured_at_parsed,
    m.cursor_id,
    m.is_measured_at_valid,
    t.measured_at_silver,
    t.is_time_imputed,
    t.time_impute_method,
    m.variable_code,
    m.value_raw,
    m.value_parsed
  from raw_long m
  join timestamps t
    on t.data_source_name = m.data_source_name
   and t.source_row_id = m.source_row_id

)

select * from long
