{{ config(
  materialized='incremental',
  unique_key=['data_source_name', 'source_row_id'],
  incremental_strategy='merge',
  indexes=[
    {'columns': ['data_source_name', 'source_row_id'], 'unique': true},
    {'columns': ['extracted_at']},
    {'columns': ['data_source_name', 'station_code', 'cursor_id']}
  ]
) }}

{%- set sources_cfg = var('measurements_sources') -%}
{%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}

with source_rows as (

  {%- for source_name in selected_source_names %}
  {%- set cfg = sources_cfg[source_name] %}

    {{ measurement_timestamps_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

)

select *
from source_rows
