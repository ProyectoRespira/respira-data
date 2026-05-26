{{ config(
  materialized='incremental',
  unique_key='code',
  incremental_strategy='merge',
  indexes=[
    {'columns': ['code'], 'unique': true},
    {'columns': ['extracted_at']}
  ]
) }}

with source_rows as (

  {%- set sources_cfg = var('measurements_sources') -%}
  {%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}

  {%- for source_name in selected_source_names %}
  {%- set cfg = sources_cfg[source_name] %}

    {{ measurement_stream_candidates_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

),

grouped as (

  select
    max(extracted_at) as extracted_at,
    station_code,
    variable_code,
    data_source_name
  from source_rows
  group by 2, 3, 4

)

select
  extracted_at,
  station_code,
  variable_code,
  data_source_name,
  (station_code || '_' || variable_code || '_' || data_source_name) as code,
  (variable_code || ' at ' || station_code) as name
from grouped
