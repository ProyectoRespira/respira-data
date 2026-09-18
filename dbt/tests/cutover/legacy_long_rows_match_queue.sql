{{ config(enabled=var('measurement_cutover_witness_enabled', false)) }}

{{ measurement_cutover_scope_guard() }}

{%- set sources_cfg = var('measurements_sources') -%}
{%- set source_name = measurement_batch_data_source() -%}
{%- set variable_count = (sources_cfg[source_name].get('variables', {}) | length) -%}

with expected as (
  select
    q.data_source_name,
    q.source_row_id,
    q.extracted_at,
    q.measured_at_silver,
    {{ variable_count }}::bigint as expected_row_count
  from {{ ref('int_measurement_timestamps_silver') }} q
  where q.cleanup_eligible_at is null
    and {{ measurement_process_row_predicate(
      'q.data_source_name',
      'q.measured_at_silver'
    ) }}
),

actual as (
  select
    legacy.data_source_name,
    legacy.source_row_id,
    legacy.extracted_at,
    legacy.measured_at_silver,
    count(*)::bigint as actual_row_count
  from {{ source('legacy_runtime_intermediate', 'int_measurements_long') }} legacy
  join expected
    on expected.data_source_name = legacy.data_source_name
   and expected.source_row_id = legacy.source_row_id
   and expected.extracted_at is not distinct from legacy.extracted_at
   and expected.measured_at_silver is not distinct from legacy.measured_at_silver
  group by 1, 2, 3, 4
)

select
  expected.*,
  coalesce(actual.actual_row_count, 0) as actual_row_count
from expected
left join actual
  using (data_source_name, source_row_id, extracted_at, measured_at_silver)
where coalesce(actual.actual_row_count, 0) <> expected.expected_row_count
