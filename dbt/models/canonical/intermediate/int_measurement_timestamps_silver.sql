{{ config(
  materialized='incremental',
  unique_key=['data_source_name', 'source_row_id'],
  incremental_strategy='merge',
  on_schema_change='sync_all_columns',
  tags=['measurement_processing_queue'],
  indexes=[
    {'columns': ['data_source_name', 'source_row_id'], 'unique': true},
    {'columns': ['data_source_name', 'extracted_at']},
    {'columns': ['data_source_name', 'measured_at_silver']},
    {'columns': ['cleanup_eligible_at', 'extracted_at']},
    {'columns': ['data_source_name', 'station_code', 'cursor_id']}
  ]
) }}

-- Operational queue contract:
-- * one row per (data_source_name, source_row_id)
-- * extracted_at is the incremental ingest checkpoint
-- * measured_at_silver coordinates half-open process windows
-- * cleanup_eligible_at is set only by post-publish orchestration gates
-- * rows remain resumable until a separate, post-success cleanup removes them
{% set sources_cfg = var('measurements_sources') -%}
{%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}

with source_rows as (

  {%- for source_name in selected_source_names %}
  {%- set cfg = sources_cfg[source_name] %}

    {{ measurement_timestamps_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

),

queue_rows as (

  select
    source_rows.*,
    null::timestamptz as cleanup_eligible_at
  from source_rows

)

select *
from queue_rows
