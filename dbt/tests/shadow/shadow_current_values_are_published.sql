with streams as (
  select id as stream_id, code
  from {{ ref('dim_streams') }}
),

current_candidates as (
  select
    values.source_row_id,
    streams.stream_id,
    values.measured_at_silver as timestamp,
    values.value_silver as value_parsed,
    values.extracted_at as ingested_at
  from {{ ref('int_measurements_values_silver_shadow') }} values
  join streams
    on streams.code = (
      values.station_code || '_' || values.variable_code || '_' || values.data_source_name
    )
  where values.measured_at_silver is not null
    and values.value_silver is not null
),

expected_keys as (
  select stream_id, timestamp, max(ingested_at) as ingested_at
  from current_candidates
  group by stream_id, timestamp
)

select expected.*
from expected_keys expected
left join {{ ref('fct_measurements_silver_shadow') }} actual
  using (stream_id, timestamp, ingested_at)
left join current_candidates candidate
  on candidate.stream_id = actual.stream_id
 and candidate.timestamp = actual.timestamp
 and candidate.ingested_at = actual.ingested_at
 and candidate.source_row_id = actual.source_row_id
 and candidate.value_parsed is not distinct from actual.value_parsed
where actual.stream_id is null
   or candidate.stream_id is null
