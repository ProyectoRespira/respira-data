{% macro validate_measurement_debug_scope() -%}
  {%- if not execute -%}
    {{ return('') }}
  {%- endif -%}

  {%- set selected_source = measurement_batch_data_source() -%}
  {%- set measured_from = measurement_batch_measured_at_from() -%}
  {%- set measured_to = measurement_batch_measured_at_to() -%}
  {%- set include_null_rows = measurement_batch_include_null_time_rows() -%}

  {%- if selected_source is none -%}
    {{ exceptions.raise_compiler_error(
      "canonical_debug_intermediate requires measurement_batch_data_source."
    ) }}
  {%- endif -%}

  {%- if include_null_rows and (measured_from is not none or measured_to is not none) -%}
    {{ exceptions.raise_compiler_error(
      "Null-time debug materialization cannot be combined with measured-time bounds."
    ) }}
  {%- endif -%}

  {%- if not include_null_rows and (measured_from is none or measured_to is none) -%}
    {{ exceptions.raise_compiler_error(
      "canonical_debug_intermediate requires both measured-time bounds or the explicit null-time flag."
    ) }}
  {%- endif -%}

  {%- if not include_null_rows and (measured_from | string) >= (measured_to | string) -%}
    {{ exceptions.raise_compiler_error(
      "measurement_batch_measured_at_from must be earlier than measurement_batch_measured_at_to."
    ) }}
  {%- endif -%}
{%- endmacro %}
