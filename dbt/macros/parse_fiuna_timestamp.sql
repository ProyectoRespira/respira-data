{% macro parse_fiuna_timestamp(date_col, time_col) -%}
  (
    -- FIUNA timestamps are emitted as fixed UTC-3 wall-clock time.
    -- Convert them to UTC explicitly instead of relying on PostgreSQL's
    -- fixed-offset AT TIME ZONE parsing, which can invert the sign.
    (
      to_timestamp(
        {{ adapter.quote(date_col) }} || ' ' || {{ adapter.quote(time_col) }},
        'DD-MM-YYYY HH24:MI'
      )::timestamp
      + interval '3 hours'
    ) at time zone 'UTC'
  )
{%- endmacro %}
