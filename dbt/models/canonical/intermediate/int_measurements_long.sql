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
{%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}

with existing_source_cutoffs as (

  {% if is_incremental() and not measurement_has_process_batch_scope() %}
  select
    data_source_name,
    max(extracted_at) as max_extracted_at
  from {{ this }}
  group by data_source_name
  {% else %}
  select
    null::text as data_source_name,
    null::timestamptz as max_extracted_at
  where false
  {% endif %}

),

selected_timestamps as (

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
  left join existing_source_cutoffs c
    on c.data_source_name = t.data_source_name
  where 1 = 1
  {% if measurement_has_process_batch_scope() %}
    and {{ measurement_process_row_predicate('t.data_source_name', 't.measured_at_silver') }}
  {% elif is_incremental() %}
    and t.extracted_at >= coalesce(c.max_extracted_at, '1970-01-01'::timestamptz)
  {% endif %}

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
