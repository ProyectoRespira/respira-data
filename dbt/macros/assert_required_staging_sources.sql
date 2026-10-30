{% macro assert_required_staging_sources(force=true) %}
  {% set state = namespace(should_validate=force, missing=[], checked=0) %}
  {% set required_source_names = var('required_staging_source_names', ['airbyte']) %}

  {% if not force %}
    {% for unique_id in selected_resources %}
      {% set node = graph.nodes.get(unique_id) %}
      {% if node
            and node.resource_type == 'model'
            and node.original_file_path.startswith('models/canonical/staging/') %}
        {% set state.should_validate = true %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {% if execute and state.should_validate %}
    {% for source_node in graph.sources.values() %}
      {% if source_node.source_name in required_source_names and source_node.config.enabled %}
        {% set relation = adapter.get_relation(
          database=source_node.database,
          schema=source_node.schema,
          identifier=source_node.identifier
        ) %}
        {% set state.checked = state.checked + 1 %}

        {% if relation is none %}
          {% do state.missing.append(
            "source('" ~ source_node.source_name ~ "', '" ~ source_node.name ~ "') -> "
            ~ source_node.database ~ '.' ~ source_node.schema ~ '.' ~ source_node.identifier
          ) %}
        {% endif %}
      {% endif %}
    {% endfor %}

    {% if state.missing %}
      {% do exceptions.raise_compiler_error(
        'Missing required staging source relations:\n- ' ~ state.missing | sort | join('\n- ')
      ) %}
    {% endif %}

    {% do log(
      'Verified ' ~ state.checked ~ ' required staging source relations for: '
      ~ required_source_names | join(', '),
      info=true
    ) %}
  {% endif %}

  select 1 as required_staging_sources_valid
{% endmacro %}
