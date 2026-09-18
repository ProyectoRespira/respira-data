{% macro measurement_shadow_anchor_mode() -%}
  {{ return(var('measurement_shadow_anchor_mode', none)) }}
{%- endmacro %}

{% macro validate_measurement_shadow_scope() -%}
  {%- if not execute -%}
    {{ return('') }}
  {%- endif -%}

  {%- set mode = measurement_shadow_anchor_mode() -%}
  {%- set source_name = measurement_batch_data_source() -%}
  {%- set extracted_from = measurement_batch_extracted_at_from() -%}
  {%- set extracted_to = measurement_batch_extracted_at_to() -%}
  {%- set measured_from = measurement_batch_measured_at_from() -%}
  {%- set measured_to = measurement_batch_measured_at_to() -%}
  {%- set source_registry = var('measurements_sources') -%}

  {%- if mode not in ['stream_state', 'prior_silver'] -%}
    {{ exceptions.raise_compiler_error(
      "measurement_shadow_anchor_mode must be 'stream_state' or 'prior_silver'."
    ) }}
  {%- endif -%}

  {%- if source_name is none or source_name not in source_registry -%}
    {{ exceptions.raise_compiler_error(
      "Shadow publishing requires measurement_batch_data_source to name a configured measurement source."
    ) }}
  {%- endif -%}

  {%- if mode == 'stream_state' -%}
    {%- if extracted_from is none -%}
      {{ exceptions.raise_compiler_error(
        "stream_state shadow publishing requires measurement_batch_extracted_at_from."
      ) }}
    {%- endif -%}
    {%- if measured_from is not none or measured_to is not none -%}
      {{ exceptions.raise_compiler_error(
        "stream_state shadow publishing does not accept measured-at bounds."
      ) }}
    {%- endif -%}
    {%- if extracted_to is not none and (extracted_from | string) >= (extracted_to | string) -%}
      {{ exceptions.raise_compiler_error(
        "measurement_batch_extracted_at_from must be earlier than measurement_batch_extracted_at_to."
      ) }}
    {%- endif -%}
  {%- else -%}
    {%- if measured_from is none or measured_to is none -%}
      {{ exceptions.raise_compiler_error(
        "prior_silver shadow publishing requires both measurement_batch_measured_at_from and measurement_batch_measured_at_to."
      ) }}
    {%- endif -%}
    {%- if extracted_from is not none or extracted_to is not none -%}
      {{ exceptions.raise_compiler_error(
        "prior_silver shadow publishing does not accept extraction bounds."
      ) }}
    {%- endif -%}
    {%- if (measured_from | string) >= (measured_to | string) -%}
      {{ exceptions.raise_compiler_error(
        "measurement_batch_measured_at_from must be earlier than measurement_batch_measured_at_to."
      ) }}
    {%- endif -%}
  {%- endif -%}

  {{ return('') }}
{%- endmacro %}

{% macro measurement_shadow_row_predicate(source_column, extracted_column, measured_column) -%}
  {%- set mode = measurement_shadow_anchor_mode() -%}
  {%- if mode == 'stream_state' -%}
    {{ return(
      measurement_source_column_predicate(source_column)
      ~ ' and '
      ~ measurement_ingest_extracted_at_predicate(extracted_column)
    ) }}
  {%- else -%}
    {{ return(
      measurement_source_column_predicate(source_column)
      ~ ' and '
      ~ measurement_process_measured_at_predicate(measured_column)
    ) }}
  {%- endif -%}
{%- endmacro %}
