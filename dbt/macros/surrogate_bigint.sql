{% macro surrogate_key_bigint(cols) %}
  (
    (
      'x' || substr({{ dbt_utils.generate_surrogate_key(cols) }}, 1, 16)
    )::bit(64)::bigint
  )
{% endmacro %}
