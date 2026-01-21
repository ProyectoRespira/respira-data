{% macro parse_fiuna_timestamp(date_col, time_col) -%}
  to_timestamp(
    {{ adapter.quote(date_col) }} || ' ' || {{ adapter.quote(time_col) }},
    'DD-MM-YYYY HH24:MI'
  ) at time zone 'UTC'
{%- endmacro %}
