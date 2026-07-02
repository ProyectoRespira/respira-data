with project_data_sources as (
  select data_source_id
  from {{ ref('bridge_project_data_sources') }}
  where project_code = 'respira_gold'
),

eligible_air_quality_stations as (
  select distinct
    stations.code as station_code
  from {{ ref('dim_streams') }} streams
  join project_data_sources pds
    on pds.data_source_id = streams.data_source_id
  join {{ ref('dim_stations') }} stations
    on stations.id = streams.station_id
  where lower(coalesce(stations.properties->>'source', '')) <> 'meteostat'
),

seeded_overrides as (
  select station_code::text as station_code
  from {{ ref('station_status_seed') }}
)

select seeded.station_code
from seeded_overrides seeded
left join eligible_air_quality_stations eligible
  on eligible.station_code = seeded.station_code
where eligible.station_code is null
