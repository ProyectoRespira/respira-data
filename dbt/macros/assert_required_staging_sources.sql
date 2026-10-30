{% macro assert_required_staging_sources(force=true) %}
  {% set state = namespace(
    should_validate=force,
    staging_models=0,
    source_ids=[],
    missing=[],
    checked=0
  ) %}

  {% if execute %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model'
            and node.config.enabled
            and node.original_file_path.startswith('models/canonical/staging/')
            and (force or node.unique_id in selected_resources) %}
        {% set state.should_validate = true %}
        {% set state.staging_models = state.staging_models + 1 %}

        {% for dependency_id in node.depends_on.get('nodes', []) %}
          {% if dependency_id.startswith('source.') and dependency_id not in state.source_ids %}
            {% do state.source_ids.append(dependency_id) %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {% if execute and state.should_validate %}
    {% for source_id in state.source_ids | sort %}
      {% set source_node = graph.sources.get(source_id) %}
      {% if source_node and source_node.config.enabled %}
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
      'Verified ' ~ state.checked ~ ' source relations used directly by '
      ~ state.staging_models ~ ' selected staging models',
      info=true
    ) %}
  {% endif %}

  select 1 as required_staging_sources_valid
{% endmacro %}
