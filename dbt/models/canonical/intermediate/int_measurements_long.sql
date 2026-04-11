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

with long as (

  {%- for source_name, cfg in sources_cfg.items() %}

    {{ unpivot_measurements_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

)

  select * from long
