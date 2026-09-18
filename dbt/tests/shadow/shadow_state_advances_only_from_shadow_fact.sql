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

advanced_state as (
  select shadow.*
  from {{ ref('measurement_stream_state_shadow') }} shadow
  left join {{ source('ops', 'measurement_stream_state') }} production
    using (data_source_name, station_code, variable_code)
  where production.data_source_name is null
     or (
       shadow.last_measured_at_silver,
       coalesce(shadow.last_cursor_id, -1),
       shadow.last_extracted_at,
       shadow.last_source_row_id
     ) > (
       production.last_measured_at_silver,
       coalesce(production.last_cursor_id, -1),
       production.last_extracted_at,
       production.last_source_row_id
     )
)

select state.*
from advanced_state state
join streams
  using (data_source_name, station_code, variable_code)
left join {{ ref('fct_measurements_silver_shadow') }} fact
  on fact.stream_id = streams.stream_id
 and fact.source_row_id = state.last_source_row_id
 and fact.timestamp = state.last_measured_at_silver
 and fact.ingested_at = state.last_extracted_at
 and fact.value_parsed is not distinct from state.last_value_silver
where fact.stream_id is null
