{{ config(enabled=var('measurement_cutover_witness_enabled', false)) }}

{{ measurement_cutover_scope_guard() }}

with queue_scope as (
  select *
  from {{ ref('int_measurement_timestamps_silver') }} q
  where q.cleanup_eligible_at is null
    and {{ measurement_process_row_predicate(
      'q.data_source_name',
      'q.measured_at_silver'
    ) }}
),

streams as (
  select id as stream_id, code
  from {{ ref('dim_streams') }}
),

candidates as (
  select
    streams.stream_id,
    legacy.measured_at_silver as timestamp,
    legacy.source_row_id,
    legacy.extracted_at as ingested_at,
    legacy.value_silver as value_parsed,
    max(legacy.extracted_at) over (
      partition by streams.stream_id, legacy.measured_at_silver
    ) as latest_ingested_at
  from {{ source('legacy_runtime_intermediate', 'int_measurements_values_silver') }} legacy
  join queue_scope queue
    on queue.data_source_name = legacy.data_source_name
   and queue.source_row_id = legacy.source_row_id
   and queue.extracted_at is not distinct from legacy.extracted_at
   and queue.measured_at_silver is not distinct from legacy.measured_at_silver
  join streams
    on streams.code = (
      legacy.station_code || '_' || legacy.variable_code || '_' || legacy.data_source_name
    )
  where legacy.measured_at_silver is not null
    and legacy.value_silver is not null
),

latest_candidates as (
  select *
  from candidates
  where ingested_at = latest_ingested_at
),

mismatches as (
  select
    expected.stream_id,
    expected.timestamp
  from latest_candidates expected
  left join {{ ref('fct_measurements_silver') }} actual
    on actual.stream_id = expected.stream_id
   and actual.timestamp = expected.timestamp
  group by expected.stream_id, expected.timestamp
  having not coalesce(bool_or(
    actual.ingested_at = expected.ingested_at
    and actual.source_row_id = expected.source_row_id
    and actual.value_parsed is not distinct from expected.value_parsed
  ), false)
)

select * from mismatches
