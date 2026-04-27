{% macro parse_fiuna_timestamp(date_col, time_col) -%}
  (
    to_timestamp(
      {{ adapter.quote(date_col) }} || ' ' || {{ adapter.quote(time_col) }},
      'DD-MM-YYYY HH24:MI'
    )::timestamp
    -- FIUNA timestamps are emitted as local UTC-3 wall-clock time.
    at time zone '-03:00'
  )
{%- endmacro %}
