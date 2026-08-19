{% macro measurement_cutover_scope_guard() -%}
  {%- if not execute -%}
    {{ return('') }}
  {%- endif -%}

  {%- set source_name = measurement_batch_data_source() -%}
  {%- set measured_from = measurement_batch_measured_at_from() -%}
  {%- set measured_to = measurement_batch_measured_at_to() -%}

  {%- if source_name is none -%}
    {{ exceptions.raise_compiler_error(
      "Measurement queue cutover witness tests require measurement_batch_data_source."
    ) }}
  {%- endif -%}
  {%- if measured_from is none or measured_to is none -%}
    {{ exceptions.raise_compiler_error(
      "Measurement queue cutover witness tests require both measured-time bounds."
    ) }}
  {%- endif -%}
  {%- if (measured_from | string) >= (measured_to | string) -%}
    {{ exceptions.raise_compiler_error(
      "measurement_batch_measured_at_from must be earlier than measurement_batch_measured_at_to."
    ) }}
  {%- endif -%}
  {{ return("") }}
{%- endmacro %}
