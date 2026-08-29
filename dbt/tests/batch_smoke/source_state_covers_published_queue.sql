with streams as (
  select
    streams.id as stream_id,
    data_sources.name as data_source_name,
    stations.code as station_code,
    variables.code as variable_code
  from {{ ref('dim_streams') }} streams
  join {{ ref('dim_data_sources') }} data_sources
    on data_sources.id = streams.data_source_id
  join {{ ref('dim_stations') }} stations
    on stations.id = streams.station_id
  join {{ ref('dim_variables') }} variables
    on variables.id = streams.variable_id
),

published_candidates as (
  select
    streams.data_source_name,
    streams.station_code,
    streams.variable_code,
    queue.measured_at_silver,
    queue.cursor_id,
    queue.extracted_at,
    queue.source_row_id
  from {{ ref('int_measurement_timestamps_silver') }} queue
  join streams
    on streams.data_source_name = queue.data_source_name
   and streams.station_code = queue.station_code
  join {{ ref('fct_measurements_silver') }} fact
    on fact.stream_id = streams.stream_id
   and fact.source_row_id = queue.source_row_id
   and fact.timestamp = queue.measured_at_silver
   and fact.ingested_at = queue.extracted_at
  where {{ measurement_runtime_queue_row_predicate(
    'queue.data_source_name',
    'queue.measured_at_silver',
    'queue.cleanup_eligible_at'
  ) }}
),

latest_candidates as (
  select *
  from (
    select
      candidates.*,
      row_number() over (
        partition by data_source_name, station_code, variable_code
        order by measured_at_silver desc, coalesce(cursor_id, -1) desc,
                 extracted_at desc, source_row_id desc
      ) as rn
    from published_candidates candidates
  ) ranked
  where rn = 1
)

select candidates.*
from latest_candidates candidates
left join {{ ref('measurement_stream_state') }} state
  using (data_source_name, station_code, variable_code)
where state.data_source_name is null
   or (
     state.last_measured_at_silver,
     coalesce(state.last_cursor_id, -1),
     state.last_extracted_at,
     state.last_source_row_id
   ) < (
     candidates.measured_at_silver,
     coalesce(candidates.cursor_id, -1),
     candidates.extracted_at,
     candidates.source_row_id
   )
