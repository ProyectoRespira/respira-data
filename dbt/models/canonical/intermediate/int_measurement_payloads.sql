{{ config(
  materialized='incremental',
  tags=['payload_audit'],
  unique_key=['data_source_name', 'source_row_id'],
  incremental_strategy='merge',
  on_schema_change='sync_all_columns',
  indexes=[
    {'columns': ['data_source_name', 'source_row_id'], 'unique': true},
    {'columns': ['extracted_at']}
  ]
) }}

{{ validate_measurement_payload_audit_scope() }}

{%- set sources_cfg = var('measurements_sources') -%}
{%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}

with payloads as (

  {%- for source_name in selected_source_names %}
  {%- set cfg = sources_cfg[source_name] %}

    {{ measurement_payloads_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

)

select *
from payloads
