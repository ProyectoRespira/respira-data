{% macro validate_measurement_payload_audit_scope() -%}
  {%- if not execute -%}
    {{ return('') }}
  {%- endif -%}

  {%- set selected_source = measurement_batch_data_source() -%}
  {%- set sources_cfg = var('measurements_sources', {}) -%}

  {%- if selected_source is none -%}
    {{ exceptions.raise_compiler_error(
      "int_measurement_payloads requires an explicit measurement_batch_data_source."
    ) }}
  {%- endif -%}

  {%- if selected_source not in sources_cfg -%}
    {{ exceptions.raise_compiler_error(
      "int_measurement_payloads received an unknown measurement_batch_data_source."
    ) }}
  {%- endif -%}
{%- endmacro %}
