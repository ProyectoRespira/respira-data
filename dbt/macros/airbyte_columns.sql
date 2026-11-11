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

{#
  Expression over a dbt_utils.union_relations result: coalesces every
  candidate column that exists in at least one of the unioned relations.
  Lets feeds synced by different Airbyte destination versions disagree on
  names like _airbyte_meta / _airbyte_metadata.
#}
{% macro coalesce_existing_columns(relations, candidate_names, fallback) -%}
  {% set found = [] %}
  {% for candidate_name in candidate_names %}
    {% for relation in relations %}
      {% if candidate_name not in found and first_existing_column(relation, [candidate_name]) %}
        {% do found.append(candidate_name) %}
      {% endif %}
    {% endfor %}
  {% endfor %}

  {% if found | length > 1 %}
    {{ return('coalesce(' ~ found | join(', ') ~ ')') }}
  {% elif found %}
    {{ return(found[0]) }}
  {% endif %}

  {{ return(fallback) }}
{%- endmacro %}
