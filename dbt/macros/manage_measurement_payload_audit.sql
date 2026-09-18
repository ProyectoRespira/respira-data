{% macro manage_measurement_payload_audit(action, confirm=false) -%}
  {%- set normalized_action = action | lower -%}
  {%- set confirmation = confirm in [true, 1, '1', 'true', 'True', 'TRUE'] -%}
  {%- set model_name = 'int_measurement_payloads' -%}
  {%- set model_node = namespace(value=none) -%}

  {%- if normalized_action not in ['quarantine', 'restore', 'drop'] -%}
    {{ exceptions.raise_compiler_error(
      "manage_measurement_payload_audit action must be quarantine, restore, or drop."
    ) }}
  {%- endif -%}

  {%- if not confirmation -%}
    {{ exceptions.raise_compiler_error(
      "manage_measurement_payload_audit requires confirm: true."
    ) }}
  {%- endif -%}

  {%- for node in graph.nodes.values() -%}
    {%- if node.resource_type == 'model' and node.name == model_name -%}
      {%- set model_node.value = node -%}
    {%- endif -%}
  {%- endfor -%}

  {%- if model_node.value is none -%}
    {{ exceptions.raise_compiler_error(
      "Payload audit model not found in manifest: " ~ model_name
    ) }}
  {%- endif -%}

  {%- if model_node.value.config.materialized != 'incremental' -%}
    {{ exceptions.raise_compiler_error(
      "Refusing payload audit retirement because int_measurement_payloads is not incremental."
    ) }}
  {%- endif -%}

  {%- set canonical_relation = adapter.get_relation(
    database=model_node.value.database,
    schema=model_node.value.schema,
    identifier=model_node.value.alias
  ) -%}
  {%- set backup_relation = adapter.get_relation(
    database=model_node.value.database,
    schema=model_node.value.schema,
    identifier=model_node.value.alias ~ '_pre_opt_in'
  ) -%}

  {%- for relation in [canonical_relation, backup_relation] -%}
    {%- if relation is not none and relation.type != 'table' -%}
      {{ exceptions.raise_compiler_error(
        "Refusing to manage non-table payload audit relation " ~ relation
        ~ " (type=" ~ relation.type ~ ")."
      ) }}
    {%- endif -%}
  {%- endfor -%}

  {%- if normalized_action == 'quarantine' -%}
    {%- if canonical_relation is none and backup_relation is none -%}
      {{ log("No payload audit table exists; quarantine is a no-op.", info=true) }}
    {%- elif canonical_relation is none or backup_relation is not none -%}
      {{ exceptions.raise_compiler_error(
        "Payload audit quarantine requires the canonical table and no existing backup."
      ) }}
    {%- else -%}
      {% call statement('quarantine_measurement_payload_audit', fetch_result=false) %}
        alter table {{ canonical_relation }} rename to
          {{ adapter.quote(canonical_relation.identifier ~ '_pre_opt_in') }}
      {% endcall %}
      {{ log("Quarantined the historical measurement payload audit table.", info=true) }}
    {%- endif -%}
  {%- elif normalized_action == 'restore' -%}
    {%- if backup_relation is none or canonical_relation is not none -%}
      {{ exceptions.raise_compiler_error(
        "Payload audit restore requires the quarantined table and a free canonical name."
      ) }}
    {%- endif -%}
    {% call statement('restore_measurement_payload_audit', fetch_result=false) %}
      alter table {{ backup_relation }} rename to {{ adapter.quote(model_node.value.alias) }}
    {% endcall %}
    {{ log("Restored the historical measurement payload audit table.", info=true) }}
  {%- else -%}
    {%- if backup_relation is none -%}
      {{ log("No quarantined payload audit table exists; drop is a no-op.", info=true) }}
    {%- else -%}
      {% call statement('drop_measurement_payload_audit_backup', fetch_result=false) %}
        drop table {{ backup_relation }}
      {% endcall %}
      {{ log("Dropped the quarantined measurement payload audit table.", info=true) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
