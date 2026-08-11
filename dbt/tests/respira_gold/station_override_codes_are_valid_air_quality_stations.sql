-- Every override must name a station this project actually serves.
--
-- The codes used to come from a reviewed CSV; they now arrive from the
-- Backoffice, and a wrong one fails silently — an override that matches no
-- station is simply dropped by the join in int_station_status_overrides. This
-- test is what makes that visible.
--
-- Unfiltered by `field`/`value` on purpose: a bad code is a mistake whatever
-- the override was meant to do.

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

recorded_overrides as (
  select distinct station_code::text as station_code
  from {{ source('respira_webapp', 'station_overrides') }}
  where nullif(station_code::text, '') is not null
)

select recorded.station_code
from recorded_overrides recorded
left join eligible_air_quality_stations eligible
  on eligible.station_code = recorded.station_code
where eligible.station_code is null
