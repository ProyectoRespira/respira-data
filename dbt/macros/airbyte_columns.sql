{% macro first_existing_column(relation, candidate_names) -%}
  {% if execute %}
    {% set relation_columns = adapter.get_columns_in_relation(relation) %}
    {% set existing_names = [] %}
    {% for column in relation_columns %}
      {% do existing_names.append(column.name | lower) %}
    {% endfor %}

    {% for candidate_name in candidate_names %}
      {% if candidate_name | lower in existing_names %}
        {{ return(candidate_name) }}
      {% endif %}
    {% endfor %}
  {% endif %}

  {{ return(none) }}
{%- endmacro %}
