{{ config(materialized='view') }}

-- Manual station shutdowns, read from the table the Django backoffice writes
-- (previously the station_status_seed.csv seed).
--
-- Only rows that hold a station *off* are emitted. The backoffice also records
-- 'active' rows — that is how it logs lifting a hold, and the audit trail the
-- product sheet asks for — but downstream (`int_project_stations`) a row being
-- present means "force inactive", so anything else must be filtered out here.
-- Lifting a hold therefore just drops the station out of this model, and its
-- derived status applies again on the next run.

with project_data_sources as (
  select data_source_id
  from {{ ref('bridge_project_data_sources') }}
  where project_code = 'respira_gold'
),

eligible_air_quality_stations as (
  select distinct
    stations.id as core_station_id,
    stations.code as station_code
  from {{ ref('dim_streams') }} streams
  join project_data_sources pds
    on pds.data_source_id = streams.data_source_id
  join {{ ref('dim_stations') }} stations
    on stations.id = streams.station_id
  where lower(coalesce(stations.properties->>'source', '')) <> 'meteostat'
),

backend_overrides as (
  select
    station_code::text as station_code,
    lower(value::text) as status,
    nullif(note::text, '') as note
  from {{ source('respira_webapp', 'station_overrides') }}
  where nullif(station_code::text, '') is not null
    and field::text = 'is_station_on'
    and lower(value::text) = 'inactive'
)

select
  eligible.core_station_id,
  overrides.station_code,
  overrides.status,
  overrides.note
from backend_overrides overrides
join eligible_air_quality_stations eligible
  on eligible.station_code = overrides.station_code
