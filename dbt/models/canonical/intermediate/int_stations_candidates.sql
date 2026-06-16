with static as (
  select
    code,
    name,
    description,
    latitude::double precision as latitude,
    longitude::double precision as longitude,
    elevation_m::double precision as elevation_m,
    status,
    properties::jsonb as properties,
    coalesce(is_pattern_station::boolean, false) as is_pattern_station
  from {{ ref('stations_static') }}
),

airelibre_latest as (
  select distinct on (station_code)
    station_code as code,
    coalesce(description, station_code) as name,
    description,
    latitude,
    longitude,
    null::double precision as elevation_m,
    'active' as status,
    jsonb_build_object('source', 'AireLibre') as properties,
    false as is_pattern_station
  from {{ ref('stg_airelibre_measurements') }}
  where latitude is not null and longitude is not null and is_measured_at_valid
  order by station_code, measured_at_parsed desc
),

mades_open_latest as (
  select distinct on (station_code)
    station_code as code,
    coalesce(
      station_name,
      source_station_code,
      source_station_id,
      station_code
    ) as name,
    concat_ws(' | ', station_city, station_type) as description,
    latitude,
    longitude,
    null::double precision as elevation_m,
    case
      when coalesce(is_active, false) then 'active'
      else 'inactive'
    end as status,
    jsonb_strip_nulls(jsonb_build_object(
      'source', 'MADES Open',
      'source_station_id', source_station_id,
      'source_station_code', source_station_code,
      'city', station_city,
      'type', station_type,
      'is_collecting_data', is_collecting_data,
      'is_under_maintenance', is_under_maintenance
    )) as properties,
    false as is_pattern_station
  from {{ ref('stg_mades_open_measurements') }}
  order by station_code, extracted_at desc, measured_at_parsed desc nulls last
),

respira_latest as (
  select
    station_code as code,
    coalesce(station_name, station_code) as name,
    location_type as description,
    latitude,
    longitude,
    null::double precision as elevation_m,
    'active' as status,
    jsonb_strip_nulls(jsonb_build_object(
      'source', 'Respira',
      'source_station_id', source_station_id,
      'station_name', station_name,
      'location_type', location_type
    )) as properties,
    false as is_pattern_station
  from {{ ref('stg_respira_stations_cache') }}
  where source_station_id is not null
),

all_candidates as (
  select * from static
  union all
  select * from airelibre_latest
  union all
  select * from mades_open_latest
  union all
  select * from respira_latest
)

select * from all_candidates
