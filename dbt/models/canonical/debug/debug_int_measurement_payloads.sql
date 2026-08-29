{{ config(
  materialized='table',
  alias='debug_int_measurement_payloads',
  tags=['debug_only', 'payload_audit']
) }}

{{ validate_measurement_payload_debug_scope() }}

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
