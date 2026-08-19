{% macro measurement_sql_string(value) -%}
  {{ return("'" ~ (value | string | replace("'", "''")) ~ "'") }}
{%- endmacro %}

{% macro measurement_batch_data_source() -%}
  {{ return(var('measurement_batch_data_source', none)) }}
{%- endmacro %}

{% macro measurement_batch_extracted_at_from() -%}
  {{ return(var('measurement_batch_extracted_at_from', none)) }}
{%- endmacro %}

{% macro measurement_batch_extracted_at_to() -%}
  {{ return(var('measurement_batch_extracted_at_to', none)) }}
{%- endmacro %}

{% macro measurement_batch_measured_at_from() -%}
  {{ return(var('measurement_batch_measured_at_from', none)) }}
{%- endmacro %}

{% macro measurement_batch_measured_at_to() -%}
  {{ return(var('measurement_batch_measured_at_to', none)) }}
{%- endmacro %}

{% macro measurement_batch_include_null_time_rows() -%}
  {%- set raw = var('measurement_batch_include_null_time_rows', false) -%}
  {{ return(raw in [true, 1, '1', 'true', 'True', 'TRUE']) }}
{%- endmacro %}

{% macro measurement_batch_unmarked_only() -%}
  {%- set raw = var('measurement_batch_unmarked_only', false) -%}
  {{ return(raw in [true, 1, '1', 'true', 'True', 'TRUE']) }}
{%- endmacro %}

{% macro measurement_source_selected(source_name) -%}
  {%- set selected_source = measurement_batch_data_source() -%}
  {{ return(selected_source is none or selected_source == source_name) }}
{%- endmacro %}

{% macro get_selected_measurement_sources(sources_cfg) -%}
  {%- set ns = namespace(selected=[]) -%}
  {%- for source_name, _cfg in sources_cfg.items() -%}
    {%- if measurement_source_selected(source_name) -%}
      {%- set ns.selected = ns.selected + [source_name] -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(ns.selected) }}
{%- endmacro %}

{% macro measurement_has_ingest_batch_scope() -%}
  {%- set selected_source = measurement_batch_data_source() -%}
  {%- set extracted_from = measurement_batch_extracted_at_from() -%}
  {%- set extracted_to = measurement_batch_extracted_at_to() -%}
  {{ return(
    selected_source is not none
    or extracted_from is not none
    or extracted_to is not none
  ) }}
{%- endmacro %}

{% macro measurement_has_process_batch_scope() -%}
  {%- set selected_source = measurement_batch_data_source() -%}
  {%- set measured_from = measurement_batch_measured_at_from() -%}
  {%- set measured_to = measurement_batch_measured_at_to() -%}
  {{ return(
    selected_source is not none
    or measured_from is not none
    or measured_to is not none
    or measurement_batch_include_null_time_rows()
  ) }}
{%- endmacro %}

{% macro measurement_source_column_predicate(column_name) -%}
  {%- set selected_source = measurement_batch_data_source() -%}
  {%- if selected_source is not none -%}
    {{ return(column_name ~ " = " ~ measurement_sql_string(selected_source)) }}
  {%- else -%}
    {{ return("1=1") }}
  {%- endif -%}
{%- endmacro %}

{% macro measurement_ingest_source_predicate(column_name) -%}
  {{ return(measurement_source_column_predicate(column_name)) }}
{%- endmacro %}

{% macro measurement_process_source_predicate(column_name) -%}
  {{ return(measurement_source_column_predicate(column_name)) }}
{%- endmacro %}

{% macro measurement_ingest_extracted_at_predicate(column_name) -%}
  {%- set extracted_from = measurement_batch_extracted_at_from() -%}
  {%- set extracted_to = measurement_batch_extracted_at_to() -%}
  {%- set clauses = [] -%}

  {%- if extracted_from is not none -%}
    {%- set clauses = clauses + [
      column_name ~ " >= cast(" ~ measurement_sql_string(extracted_from) ~ " as timestamptz)"
    ] -%}
  {%- endif -%}

  {%- if extracted_to is not none -%}
    {%- set clauses = clauses + [
      column_name ~ " < cast(" ~ measurement_sql_string(extracted_to) ~ " as timestamptz)"
    ] -%}
  {%- endif -%}

  {{ return(clauses | join(" and ") if clauses else "1=1") }}
{%- endmacro %}

{% macro measurement_process_measured_at_predicate(column_name) -%}
  {%- set measured_from = measurement_batch_measured_at_from() -%}
  {%- set measured_to = measurement_batch_measured_at_to() -%}
  {%- set include_null_rows = measurement_batch_include_null_time_rows() -%}

  {%- if include_null_rows -%}
    {{ return(column_name ~ " is null") }}
  {%- endif -%}

  {%- set clauses = [] -%}

  {%- if measured_from is not none -%}
    {%- set clauses = clauses + [
      column_name ~ " >= cast(" ~ measurement_sql_string(measured_from) ~ " as timestamptz)"
    ] -%}
  {%- endif -%}

  {%- if measured_to is not none -%}
    {%- set clauses = clauses + [
      column_name ~ " < cast(" ~ measurement_sql_string(measured_to) ~ " as timestamptz)"
    ] -%}
  {%- endif -%}

  {{ return(clauses | join(" and ") if clauses else "1=1") }}
{%- endmacro %}

{% macro measurement_ingest_row_predicate(source_column_name, extracted_at_column_name) -%}
  {{ return(
    measurement_ingest_source_predicate(source_column_name)
    ~ " and "
    ~ measurement_ingest_extracted_at_predicate(extracted_at_column_name)
  ) }}
{%- endmacro %}

{% macro measurement_process_row_predicate(source_column_name, measured_at_column_name) -%}
  {{ return(
    measurement_process_source_predicate(source_column_name)
    ~ " and "
    ~ measurement_process_measured_at_predicate(measured_at_column_name)
  ) }}
{%- endmacro %}

{% macro measurement_runtime_queue_row_predicate(source_column_name, measured_at_column_name, cleanup_eligible_column_name) -%}
  {%- if flags.FULL_REFRESH -%}
    {{ return("1=1") }}
  {%- elif measurement_has_process_batch_scope() -%}
    {%- set process_scope = measurement_process_row_predicate(
      source_column_name,
      measured_at_column_name
    ) -%}
    {%- if measurement_batch_unmarked_only() -%}
      {{ return(
        process_scope ~ " and " ~ cleanup_eligible_column_name ~ " is null"
      ) }}
    {%- else -%}
      {{ return(process_scope) }}
    {%- endif -%}
  {%- else -%}
    {{ return(cleanup_eligible_column_name ~ " is null") }}
  {%- endif -%}
{%- endmacro %}

{% macro measurement_source_incremental_predicate(source_name, extracted_at_col) -%}
  {%- if measurement_has_ingest_batch_scope() -%}
    {{ return(measurement_ingest_extracted_at_predicate(extracted_at_col)) }}
  {%- elif is_incremental() -%}
    {{ return(
      extracted_at_col
      ~ " >= (select coalesce(max(extracted_at), '1970-01-01'::timestamptz)"
      ~ " from "
      ~ this
      ~ " where data_source_name = "
      ~ measurement_sql_string(source_name)
      ~ ")"
    ) }}
  {%- else -%}
    {{ return("1=1") }}
  {%- endif -%}
{%- endmacro %}
