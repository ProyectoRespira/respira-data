with static as (
  select
    code,
    name,
    description,
    latitude::double precision as latitude,
    longitude::double precision as longitude,
    elevation_m::double precision as elevation_m,
    status,
    properties::jsonb as properties
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
    jsonb_build_object('source', 'AireLibre') as properties
  from {{ ref('stg_airelibre_measurements') }}
  where latitude is not null and longitude is not null and is_measured_at_valid
  order by station_code, measured_at_parsed desc
),

all_candidates as (
  select * from static
  union all
  select * from airelibre_latest
)

select * from all_candidates
