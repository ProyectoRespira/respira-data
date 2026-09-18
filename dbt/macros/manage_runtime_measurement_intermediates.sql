{% macro manage_runtime_measurement_intermediates(action, confirm=false) -%}
  {%- set normalized_action = action | lower -%}
  {%- set confirmation = confirm in [true, 1, '1', 'true', 'True', 'TRUE'] -%}
  {%- set model_names = ['int_measurements_long', 'int_measurements_values_silver'] -%}
  {%- set model_nodes = namespace(by_name={}) -%}

  {%- if normalized_action not in ['quarantine', 'restore', 'drop'] -%}
    {{ exceptions.raise_compiler_error(
      "manage_runtime_measurement_intermediates action must be quarantine, restore, or drop."
    ) }}
  {%- endif -%}

  {%- if not confirmation -%}
    {{ exceptions.raise_compiler_error(
      "manage_runtime_measurement_intermediates requires confirm: true."
    ) }}
  {%- endif -%}

  {%- for node in graph.nodes.values() -%}
    {%- if node.resource_type == 'model' and node.name in model_names -%}
      {%- do model_nodes.by_name.update({node.name: node}) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- for model_name in model_names -%}
    {%- if model_name not in model_nodes.by_name -%}
      {{ exceptions.raise_compiler_error("Runtime model not found in manifest: " ~ model_name) }}
    {%- endif -%}
    {%- if model_nodes.by_name[model_name].config.materialized != 'ephemeral' -%}
      {{ exceptions.raise_compiler_error(
        "Refusing relation retirement because " ~ model_name ~ " is not ephemeral."
      ) }}
    {%- endif -%}
  {%- endfor -%}

  {%- set relations = namespace(canonical=[], backup=[]) -%}
  {%- for model_name in model_names -%}
    {%- set model_node = model_nodes.by_name[model_name] -%}
    {%- set canonical_relation = adapter.get_relation(
      database=model_node.database,
      schema=model_node.schema,
      identifier=model_node.alias
    ) -%}
    {%- set backup_relation = adapter.get_relation(
      database=model_node.database,
      schema=model_node.schema,
      identifier=model_node.alias ~ '_pre_ephemeral'
    ) -%}
    {%- set relations.canonical = relations.canonical + [canonical_relation] -%}
    {%- set relations.backup = relations.backup + [backup_relation] -%}
  {%- endfor -%}

  {%- set canonical_count = relations.canonical | reject('none') | list | length -%}
  {%- set backup_count = relations.backup | reject('none') | list | length -%}

  {%- if canonical_count not in [0, 2] or backup_count not in [0, 2] -%}
    {{ exceptions.raise_compiler_error(
      "Refusing ambiguous partial relation state: both runtime relations must move together."
    ) }}
  {%- endif -%}

  {%- for relation in relations.canonical + relations.backup -%}
    {%- if relation is not none and relation.type != 'table' -%}
      {{ exceptions.raise_compiler_error(
        "Refusing to manage non-table relation " ~ relation ~ " (type=" ~ relation.type ~ ")."
      ) }}
    {%- endif -%}
  {%- endfor -%}

  {%- if normalized_action == 'quarantine' -%}
    {%- if canonical_count == 0 and backup_count == 0 -%}
      {{ log("No legacy runtime intermediate tables exist; quarantine is a no-op.", info=true) }}
    {%- elif canonical_count != 2 or backup_count != 0 -%}
      {{ exceptions.raise_compiler_error(
        "Quarantine requires both canonical tables and no existing backups."
      ) }}
    {%- else -%}
      {%- for relation in relations.canonical -%}
        {% call statement('quarantine_' ~ loop.index, fetch_result=false) %}
          alter table {{ relation }} rename to {{ adapter.quote(relation.identifier ~ '_pre_ephemeral') }}
        {% endcall %}
      {%- endfor -%}
      {{ log("Quarantined legacy runtime measurement intermediate tables.", info=true) }}
    {%- endif -%}
  {%- elif normalized_action == 'restore' -%}
    {%- if canonical_count != 0 or backup_count != 2 -%}
      {{ exceptions.raise_compiler_error(
        "Restore requires both quarantined tables and no canonical physical tables."
      ) }}
    {%- endif -%}
    {%- for relation in relations.backup -%}
      {%- set restored_name = relation.identifier | replace('_pre_ephemeral', '') -%}
      {% call statement('restore_' ~ loop.index, fetch_result=false) %}
        alter table {{ relation }} rename to {{ adapter.quote(restored_name) }}
      {% endcall %}
    {%- endfor -%}
    {{ log("Restored quarantined runtime measurement intermediate tables.", info=true) }}
  {%- else -%}
    {%- if canonical_count == 0 and backup_count == 0 -%}
      {{ log("No quarantined runtime intermediate tables exist; drop is a no-op.", info=true) }}
    {%- elif canonical_count != 0 or backup_count != 2 -%}
      {{ exceptions.raise_compiler_error(
        "Drop requires both quarantined tables and no canonical physical tables."
      ) }}
    {%- else -%}
      {%- for relation in relations.backup -%}
        {% call statement('drop_' ~ loop.index, fetch_result=false) %}
          drop table {{ relation }}
        {% endcall %}
      {%- endfor -%}
      {{ log("Dropped quarantined runtime measurement intermediate tables.", info=true) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
