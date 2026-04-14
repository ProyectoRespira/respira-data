{{ config(
  materialized='incremental',
  unique_key=['data_source_name', 'source_row_id'],
  incremental_strategy='merge',
  indexes=[
    {'columns': ['data_source_name', 'source_row_id'], 'unique': true},
    {'columns': ['extracted_at']}
  ]
) }}

{%- set sources_cfg = var('measurements_sources') -%}

with payloads as (

  {%- for source_name, cfg in sources_cfg.items() %}

    {{ measurement_payloads_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

)

select *
from payloads
